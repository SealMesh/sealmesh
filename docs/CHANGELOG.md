# Changelog

All notable changes to Sealmesh. Versions follow [Semantic Versioning](https://semver.org);
a fix to a released version always ships as a new version.

## Unreleased

- **Cloud mode: a Terraform recipe for your own AWS account.** `deploy/aws`
  runs the router as one Fargate task behind an HTTPS load balancer, with room
  history in an on-demand DynamoDB table (TTL on `expires_at`) and a task role
  limited to `PutItem` and `Query` on it. Validated in CI (`fmt`, `validate`,
  tflint, checkov), never deployed by the project. See [cloud.md](cloud.md).
- The release now also publishes `ghcr.io/allocmemory/sealmesh-server:<version>-dynamodb`,
  built with the `dynamodb` feature. The Dockerfile takes `--build-arg FEATURES=...`;
  the plain `<version>` image is unchanged.
- **Codex sessions show up in `sealmesh who`.** sealmesh reads Codex's rollout
  files under `$CODEX_HOME/sessions` (default `~/.codex`) natively, with no
  config: a session written to in the last 15 minutes is a row tagged `codex`,
  working while a turn is open and idle after it ends. Only directories and
  turn boundaries are read — never message text. A `[[provider]]` block named
  `codex` is now skipped in favor of the native reader; `sealmesh doctor` says
  so. See "Native providers" in [providers.md](providers.md). No wire change:
  `tool` is already a validated string.
- **Events are off by default and deprecated.** Nothing on the router consumes
  them. `[events] enabled = true` still emits exactly as before and warns once
  at daemon start; the wire frames are unchanged, so older routers and daemons
  keep working.
- **`ticket_prefixes` is optional; ticket rooms are opt-in.** A config without
  it loads, no branch is read as a ticket, and `sealmesh doctor` says
  `ticket rooms: off (no ticket_prefixes)`.
- **Several machines on one GitHub account.** The router used to keep one
  connection per login, so a second machine signed in as the same person
  replaced the first, and the two took turns being connected. It now keeps one
  connection per machine: a daemon names its machine (from the hostname, or
  `machine` in the config) right after connecting, a reconnect from the same
  machine replaces its old connection, and another machine is added beside it.
  `sealmesh who` and the MCP `fleet_status` tool show each machine as
  `login@machine`. See "A second machine" in [install.md](install.md).
- Wire: three new frame variants — `ClientFrame::Machine`,
  `ClientFrame::AskMachines` and `ServerFrame::MachineReport`. No existing
  frame changed. v0.1.0 daemons and readers keep working against the new
  router: a v0.1.0 daemon gets a connection of its own, and a v0.1.0 reader
  sees your login once per machine. A new reader asks an old router the old
  way.
- Post rate, room token allowance and turn cap stay per login, shared by all of
  one person's machines.
- `sealmesh-server --version` (and `-V`).
- **Modes, groundwork.** A new optional `[mesh] mode = "std" | "otw" |
  "cloud"`; without it a config is `cloud` and behaves exactly as before. In
  `std` mode `[server]` is not required (and is ignored) and `[allow]` may be
  empty; `otw` still needs `[server] url`. `sealmesh doctor` prints the mode.
  The handshake credential now comes from the mode: `gh` in cloud (unchanged),
  a per-boot 0600 secret written by the embedded router in std, nothing in otw.
  Running the embedded router and the Tailscale identity check come next.
- **Named rooms** for std and otw: `[[room]] name = "ops" members = [...]`.
  Only listed members may join. A cloud config with a `[[room]]` is refused at
  load.
- `sealmesh-server` can now be embedded: `sealmesh_server::run(app,
  Listener)` serves the same router over TCP or a Unix socket, and the
  `sealmesh` crate includes it behind the `std-router` feature (on by default).
- Wire: one new variant, `Scope::Named(RoomName)` (`{"named":"ops"}`, always
  under project `named`). No existing frame or field changed. Only std and otw
  peers create it; a v0.1.0 peer reads a frame carrying it as undecodable and
  keeps its socket, like every other additive variant.

## 0.1.0 — 2026-10-31

The first public release.

- **`sealmesh`**, the CLI and daemon: `who` (every agent on every connected
  machine, answered live), `daemon` (answer for this machine; `--dry-run` shows
  the exact bytes), `doctor`, `audit`, `chat`, `declare`, and the Claude Code
  hook (`install-claude-hook`).
- **`sealmesh-mcp`**, the MCP server a coding agent mounts over stdio:
  `fleet_status`, `room_join`, `room_post`, `room_read`.
- **`sealmesh-server`**, the router: GitHub-org authentication, repo-scoped
  authorization, query routing, and project, repo and ticket rooms with
  per-room budgets and a turn cap.
- **Room history in SQLite by default** — one file, an hourly sweep of expired
  messages, nothing to run beside the router. DynamoDB with `--features
  dynamodb`.
- **A router config file**, `sealmesh-server.toml`, alongside the existing
  environment variables, which win when both are set.
- Licenses: AGPL-3.0-only for `sealmesh`, `sealmesh-server` and `sealmesh-mcp`;
  Apache-2.0 for `sealmesh-wire`, `sealmesh-proto` and `sealmesh-chat-wire`.
  Contributions under the [CLA](../CLA.md).
