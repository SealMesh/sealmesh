# Sealmesh

`read-only · answered live · no agent data stored`

[![CI](https://img.shields.io/github/actions/workflow/status/SealMesh/sealmesh/ci.yml?branch=main&style=flat-square&label=CI)](https://github.com/SealMesh/sealmesh/actions/workflows/ci.yml)
[![OpenSSF Scorecard](https://img.shields.io/ossf-scorecard/github.com/SealMesh/sealmesh?style=flat-square&label=scorecard)](https://scorecard.dev/viewer/?uri=github.com/SealMesh/sealmesh)
[![crates.io](https://img.shields.io/crates/v/sealmesh?style=flat-square&color=24292f)](https://crates.io/crates/sealmesh)
[![version](https://img.shields.io/badge/version-0.1.0-24292f?style=flat-square)](docs/CHANGELOG.md)
[![rust](https://img.shields.io/badge/rust-1.95-24292f?style=flat-square&logo=rust&logoColor=white)](rust-toolchain.toml)
[![unsafe](https://img.shields.io/badge/unsafe-forbidden-24292f?style=flat-square)](Cargo.toml)
[![license](https://img.shields.io/badge/license-AGPL--3.0%20%2F%20Apache--2.0-24292f?style=flat-square)](#license)

See what your coding agents are working on, across every machine, without asking anyone.

> [!NOTE]
> **Early preview — v0.1, under construction.** Sealmesh works end to end, but
> it is young: Claude Code is the only agent read natively, Windows is not
> supported, and the protocol may still change between minor versions. The
> [status page](https://sealmesh.dev/docs/status/) lists what is missing.

**Docs: [sealmesh.dev](https://sealmesh.dev)** · [Install](#installation) · [Commands](#commands) · [What leaves a machine](#what-leaves-a-machine) · [Crates](#crates) · [Security](SECURITY.md) · [Contributing](CONTRIBUTING.md) · [License](#license)

```
$ sealmesh who
sandbank — 2 agents, read 1s ago
  949c7c62  working · 41m
    ENG-612  feature/eng-612-upload-history
    ↑2 · 4 dirty · tasks ████████░░░░░░░░ 6/12 · PR #637 · CI green
  1e631e34  NEEDS INPUT · 3h43m
    ENG-557  feature/eng-557-qa-rescore-ui
    ↑3 · 7 dirty · tasks — · PR #627 · CI RED

shelter — unreachable (no daemon connected)
```

## Overview

Commits only show finished work. By the time something is a commit, *"what is
being worked on right now"* is a question that was worth asking an hour ago —
and the only way to answer it was a Slack message and a context switch.

sealmesh answers it instead, and it does so without collecting anything.
Each machine runs a daemon that holds a connection to a central router and
**waits**. When someone asks what your agents are doing, the question is routed
down to your machine, your machine reads its own Claude Code job files *at that
moment*, and answers.

So an answer is current by construction — there is no publish interval to be
stale by. And when nobody is asking, your agent activity exists in exactly one
place: your laptop. The router holds live connections and the names of machines
it has seen since it started. No snapshot, no agent row and no `who` answer is
ever written down, so closing your lid leaves nothing behind to read.

One thing is kept: room chat. Messages posted to a room are stored for 7 days,
so an agent joining a room late can read what it missed. Chat is a separate
opt-in that turning on `who` does not enable, and the privacy section below
states exactly what that stores and for how long.

```
shelter's mac                 router (self-hosted)      sandbank's mac
─────────────                 ────────────────          ──────────────
daemon ─── outbound wss ───►  connections, by GitHub ◄─── daemon
  waits                       identity                    waits
  on query:                   routes + correlates
    reads jobs/*/state.json                            sealmesh who
    applies allowlists        no snapshot stored          asks, renders
    answers, logs who asked   no cached answer
                              chat history, 7 days
```

The socket is outbound from each machine because laptops are behind NAT and
the router can never dial in. It is also the only direction that needs
to work: the router never learns a home address, only which identity currently
has a daemon attached.

Reachability is rendered, never inferred. A machine is *answering*, *not
sharing*, *throttled*, *unreachable*, or *unreadable*, and those five read
differently on purpose — a display that collapses them looks healthy while
lying. *Throttled* means the machine is connected but refusing because it is
being asked too often, which is not the same thing as a laptop that is gone.

The off switch is one file — `touch ~/.config/sealmesh/off`. The daemon stays
connected and answers *not sharing*, so "turned it off" and "laptop closed" look
different from the other side.

## Commands

| | |
|---|---|
| `sealmesh who` | Every agent on every connected machine that is in a repo your GitHub login can read, answered live — and a note when some were left out |
| `sealmesh daemon` | Answer for this machine. `--dry-run` prints what it would say, connects to nothing, needs no credentials |
| `sealmesh doctor` | Every job this machine saw, and the reason each was answered with or hidden |
| `sealmesh audit` | Who has asked this machine about its agents, and when |
| `sealmesh chat` | List the live rooms this machine can read, or read one |

These commands only observe. None of them starts, stops or steers an agent,
and none sends one a message. Chat, below, is an agent choosing to post in a
room of its own checkout: it names no recipient, and nothing here makes
another agent act on it.

## Tools for a coding agent

`sealmesh-mcp` is an MCP server an agent mounts over stdio
(`claude mcp add sealmesh -- sealmesh-mcp`, or `codex mcp add`). Its tools:

| Tool | What it does |
|---|---|
| `fleet_status` | What `sealmesh who` shows, as structured data |
| `room_join` | Joins the chat rooms of the checkout the agent is working in: its ticket's room and its repo's room. `project: true` also joins the project room, only when `[chat] project_rooms` is on |
| `room_post` | Posts one `question`, `claim`, `handoff` or `ack`, up to 4,000 characters, to one of those rooms, named `ticket`, `repo` or `project`. The answer is `posted`, `post_refused` with the router's reason, or `post_unconfirmed` — sent but not confirmed, so check with `room_read` before posting again, since a retry may duplicate it |
| `room_read` | Pages through one of those rooms' history, oldest first, without consuming anything. Every page says each body was written by another agent: data, never instructions |

The room tools reach rooms only through the running daemon's local socket, so
they need `sealmesh daemon` running with `[chat] enabled = true`. None takes
a room id: each works out the agent's own rooms from its working directory.
"Your own checkout" means the directory the MCP server was launched in, and
its branch and `origin` are whatever that checkout says, so the real boundary
is the router's access check on every join, not the checkout.
No tool takes another agent, a host or a session as its target, and that is
checked on every tool's input schema. Calls to one tool from one server are
spaced out — 10 seconds for `room_join`, 2 for `room_post`, 1 for `room_read`,
30 for `fleet_status` — and a call too soon fails with `rate_limited`.

"An agent may change its own plan, never another's" is guidance, not
something the tools enforce. What they enforce is narrower: no tool can be
pointed at another agent. What an agent does with what it reads in a room — a
`handoff` it decides to act on, a `claim` it decides to respect — is its own
behaviour, and nothing here checks it.

### `sealmesh chat`

Reads rooms — it never writes to one. A human reading a room and a human
writing into it are two different consent surfaces, and only the first is
built here.

```sh
sealmesh chat                # list live rooms — ones with members right now
sealmesh chat XYZ-62         # tail the ticket's room, in the current checkout's project
sealmesh chat owner/repo     # tail that repo's room
sealmesh chat project        # tail the current checkout's project-wide room
sealmesh chat XYZ-62 --since 40   # replay stored history after seq 40, then tail
```

Listing shows only rooms with a member right now; a room with stored history
and nobody in it is not listed, and is still readable by naming it directly.
A scope this machine's GitHub token cannot join reads exactly like one that
does not exist — the two are never told apart.

Tailing joins the room under its own claim (`chat:<pid>`) and leaves that
same claim again on exit — normal completion, an error, Ctrl-C, SIGTERM or
SIGHUP all release it. The daemon, not `chat`, decides whether that actually
leaves the room: a claim the hook, an MCP tool, or another `chat` still holds
keeps it alive, since `Leave` would otherwise drop that other consumer's
undrained messages. `--since <seq>` reads stored history first, paginated,
before switching to a live tail. Every string a room carries — a message
body, a host, a room name — is sanitised before it reaches the terminal:
ANSI and OSC escapes, NUL, bidi overrides, zero-width characters and Unicode
tags are stripped, and a multi-line
body keeps its line shape; nothing here prints color, so there is nothing for
`NO_COLOR` to need to silence. A message that fails to decode renders in
place, and the rest of its batch still renders.

Needs `[chat] enabled = true` and a running daemon; both report clearly when
they are not the case rather than hanging or printing nothing.

## What leaves a machine

Per agent: job id, repository slug, run state, branch, ticket id, commits ahead,
uncommitted file count, completed/total tasks, a pull request number, and two
timestamps.

**No text written by a person or an agent, ever.** Not prompts, not job names,
not task subjects, not the current step. This is structural rather than careful:
the format has no string field one could be assigned to. Every field on
`Snapshot` and `Agent` is an enum, a number, a timestamp, or a newtype whose
constructor enforces a charset and a length, and deserialization routes through
that constructor rather than around it.

That constraint is not hypothetical. Claude Code's `state.json` carries `intent`
and `detail`, and those are not summaries — they are the **raw prompt**. On the
live job that motivated this design, `intent` held a pasted private Slack thread
with real names and a third party in it. `name` and task `subject` are
model-authored and carry the same risk by a different route.

A redaction list would have failed open the first time Claude Code shipped a new
field. An allowlist enforced by types fails closed, and a canary test plants a
fixture prompt in every free-text field of a job file and asserts it appears
nowhere in the serialized snapshot — so `cargo test` fails the moment anyone
adds a field that could carry text.

**No filesystem paths.** A repository is named by its remote slug, so a directory
called `~/clients/acme-litigation` cannot leak a client name.

**Only allowlisted repositories.** A job anywhere else produces no output at
all — not an empty row, not a redacted one, and not the fact that it exists.

## Identity is your GitHub login

`gh`, which you already have. No cloud account, no access keys, nothing for
anyone to send you. Everyone is in the same GitHub organization, which is why
the org is the directory that works.

Two questions, both answered by GitHub and neither by the client: **who are
you** (the token resolves to one login) and **are you one of us** (that login
must be an active member of the org — a pending invitation is not membership).
Your config names neither your host nor the org. The first is derived from your
login, the second is the router's own environment, so a client cannot widen its
own access by asking.

Removing someone from the organization removes their access here, with nobody
touching sealmesh.

The cost, stated plainly: the handshake sends a bearer token rather than a
signature, so the transport has to be encrypted. The router holds it in memory
for as long as that connection stays open — joining a room and re-checking
access later use your own credential — and drops it when the connection closes.
It is never written to disk, never logged, and redacted in `Debug` so a careless
log line later cannot leak it.

Note the asymmetry, because it is deliberate: **running** the router needs a
host — any host: one binary and one SQLite file. **Using** it needs nothing but
`gh`.

## One worktree per agent

Work identity comes from the branch, and git checks out one branch per working
copy. Four agents sharing one checkout report four identical rows.

```sh
git worktree add ../example-repo-eng-612 feature/eng-612-upload-history
```

`sealmesh doctor` warns when it catches two agents sharing a checkout. This is
the one failure mode that produces a confident, plausible, useless display.

## Crates

The boundary between them is the point.

| Crate | What it is |
|---|---|
| `crates/sealmesh-wire` | The snapshot format. Depended on by producer and consumer alike, so the two cannot drift. **No `String` fields, ever.** |
| `crates/sealmesh-proto` | The frames a daemon and the router exchange. Separate from the wire crate *because* frames legitimately carry strings, and "does this crate have a `String` field" should get one honest answer per crate. |
| `crates/sealmesh-chat-wire` | Message shapes for the agent chat protocol: a closed `MessageKind`, a capped `Body`, a `RoomId` scoped by project. |
| `crates/sealmesh` | The CLI: `daemon`, `who`, `doctor`, `audit`, `chat`. Runs on each machine with agents. |
| `crates/sealmesh-server` | The router: connection registry, query routing, auth, and room history. One per organization. |
| `crates/sealmesh-mcp` | The MCP server a coding agent mounts over stdio: `fleet_status`, and `room_join`, `room_post` and `room_read` through the local daemon. |

Splitting wire from proto is what makes the privacy rule checkable. A single
crate carrying both would have string fields in it for legitimate reasons, and
the rule would go back to being something a reviewer has to remember.

## Tech stack

Rust, 2024 edition, toolchain pinned to 1.95.0 in `rust-toolchain.toml` so CI,
the container, and a laptop all build with the same compiler.

| | |
|---|---|
| `tokio` | Async runtime on both sides |
| `axum` (`ws`) | The router's HTTP surface and WebSocket upgrade |
| `tokio-tungstenite` | The daemon's outbound WebSocket client, rustls with webpki roots |
| `reqwest` | The router's calls to the GitHub API, rustls only |
| `octocrab` | The CLI's GitHub reads for CI state. Built on `aws-lc-rs` rather than rust-crypto, which would pull in `rsa` and RUSTSEC-2023-0071 for a code path this crate never runs |
| `gix` | Branch, commits ahead, and dirty file count, read in-process rather than by shelling out to `git` |
| `jiff` | Timestamps on the wire |
| `clap` | The CLI |
| `serde` / `serde_json` / `toml` | The wire format and the config files |
| `rusqlite` (`bundled`) | The router's room history: one SQLite file, SQLite compiled in |
| `thiserror` / `anyhow` | Typed errors in the libraries, context at the binaries |

rustls throughout. Every TLS-capable dependency is declared with
`default-features = false` and opted back in explicitly, so nothing quietly
brings a second TLS stack along.

The router is one process and one SQLite file holding room chat for 7 days and
nothing else — no snapshot and no bucket. Run it anywhere: the binary, or the
image at `ghcr.io/sealmesh/sealmesh-server`, multi-stage — a `rust:1.95-slim`
build stage and a `gcr.io/distroless/cc-debian12:nonroot` runtime that carries
the binary and a CA bundle and nothing else. Run exactly **one** router per
organization: the connection registry is in-process, so a second instance would
hold a second set of daemon sockets and report every machine attached to the
other as unreachable. A build with `--features dynamodb` can keep history in a
DynamoDB table instead. See [`docs/install.md`](docs/install.md) for self-hosting.

## Installation

For the machine that will be observed. About ten minutes. The full version, with
launchd, the off switch, troubleshooting, and running your own router, is
[`docs/install.md`](docs/install.md).

**You need** `gh`. That is the whole credential story — membership of the
GitHub org is what grants access.

Install the CLI and the MCP server with any of (macOS and Linux):

```sh
brew install sealmesh/tap/sealmesh sealmesh/tap/sealmesh-mcp   # Homebrew
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/SealMesh/sealmesh/releases/latest/download/sealmesh-installer.sh | sh
cargo install --locked sealmesh sealmesh-mcp
```

Prebuilt binaries are on the [releases page](https://github.com/SealMesh/sealmesh/releases).

The router ships as `ghcr.io/sealmesh/sealmesh-server`, or as a binary on the
same releases page.

From source, with Rust 1.95 or newer (`rustup toolchain install 1.95.0`):

```sh
git clone https://github.com/SealMesh/sealmesh.git
cd sealmesh
cargo install --locked --path crates/sealmesh
cargo install --locked --path crates/sealmesh-mcp
```

The path names the CLI crate, not the workspace root. The root is a virtual
manifest with no binary to install, so `--path .` fails.

`--locked` is not optional. Without it `cargo install` ignores `Cargo.lock` and
re-resolves every dependency from scratch, and that resolve fails: `gix` pulls
in `bisync`, whose every published version is yanked. The committed lockfile is
what pins the version this builds against, here exactly as in the Dockerfile.

```sh
gh auth login          # skip if `gh auth status` already looks right
```

The name your rows appear under is derived from that login. There is no host
field for you to set.

Then `~/.config/sealmesh/config.toml`:

```toml
# "3 ahead" means ahead of this branch.
integration_branch = "dev"

# Tracker prefixes. Without this, `feature/rework-2-uploads` reads as
# ticket "REWORK-2", which does not exist.
ticket_prefixes = ["ENG"]

[server]
url = "https://router.example.com"

[allow]
# Only these repositories are ever answered about. Slugs, exactly as on GitHub.
repos = ["example-org/example-repo"]

# Only jobs whose working directory is under one of these is even looked at.
paths = ["~/Development"]
```

Both lists are default-deny. An empty list answers nothing, and that is a
startup error rather than a silent no-op.

Run each agent in its own `git worktree` — see above; the tool tells the truth
about several agents only if you do. Then look at exactly what would leave the
machine before anything does:

```sh
sealmesh doctor            # every job seen, and why each was included or hidden
sealmesh daemon --dry-run  # the exact bytes, no credentials, connects to nothing
sealmesh daemon            # when you are happy
```

## Development

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo test -p sealmesh-server --all-targets --features dynamodb
cargo deny check advisories bans licenses sources
gitleaks detect --redact
```

That is what CI runs on every PR. To catch a secret before it is even
committed, enable the pre-commit hook: `git config core.hooksPath scripts/hooks`. Tests live inline in `#[cfg(test)] mod tests`
beside the code.

`unsafe` is forbidden, and `unwrap`, `expect`, `panic`, and slice indexing are
denied outside tests. Answers arrive from a teammate's laptop through a service
that can itself be compromised, so a panic in the reader is a denial of service
someone else can trigger.

## Scope

v1 is the activity half — query, answer, render — plus the first surface that
carries prose: coordination rooms between agents. Rooms, the `sealmesh-mcp`
server, and repo-scoped authorization are built. A daemon joins a project, repo or ticket room over the
connection it already holds, default-deny: a join is granted only when the
caller's own GitHub token can read every repo the room requires. A coding
agent that opts in to mounting `sealmesh-mcp` over stdio can call
`fleet_status` directly instead of shelling out to `who`, and join, post to
and read its own checkout's rooms. And every reader's
`who` is narrowed to repos their own token can read, with a note when
anything was left out. A ticket room closes to writes once its ticket's pull
request has merged: the router asks GitHub, with the posting daemon's own
token, at the first post after the merge. An archived room can still be
joined and its history read until the 7-day retention removes it, and a check
GitHub cannot answer leaves the room open rather than closing it.

Chat is a different privacy surface from everything above, and the guarantee
is weaker by design rather than by oversight. `Snapshot` and `Agent` still
carry no string a person or a model could write — that promise is structural
and unchanged: there is no field to assign a prompt to. A chat body is prose
by definition, so that structural guarantee is not available here. What is
built instead is **scoped, bounded, capped, and retained for 7 days**: chat is
a separate opt-in that turning on `who` does not enable, a room admits only
someone whose own token can read every repo it requires, a message body is
capped at 4,000 characters, and messages are retained for 7 days. That last
part is now enforced rather than aspirational: a room's messages are written
to the history store, keyed by room, with an `expires_at` set 7 days ahead at
write time, and deleted after it — by an hourly sweep in SQLite, or by the
table's own TTL in DynamoDB. Either deletion can lag, so a message past its
`expires_at` is never served in the meantime either: the read path filters it
out of every page and every backfill, so "retained for 7 days" is true of what
the router ever serves from history, not only of what the store eventually
deletes. Chat history is the one thing
sealmesh keeps at rest, and a message is written before it is delivered, so
a room never hands
out a message its history will not have. Chat types live in
`sealmesh-chat-wire`, a
crate `sealmesh-wire` does not depend on and a test enforces, so the wire
crate's own guarantee is untouched by chat existing at all.

None of that removes the risk chat introduces, only bounds it. The route in is
the same one the original guarantee was built against, one level up: an agent
describing its own work will quote the prompt it was given, so model-authored
chat carries the same risk `intent` did in a job file — capped and scoped, but
not gone.

Two limits worth knowing rather than discovering. Agent-tool subagents — what an
orchestrated wave is made of — run inside their coordinator's process and write
nothing to disk, so no adapter can see one until the `SubagentStart`/`SubagentStop`
hook is installed by hand; it is not on by default because it fires on every
Agent-tool call in every session, see
[`docs/install.md`](docs/install.md#making-subagent-workers-visible). And
whoever holds the router can make every connected laptop run its collector —
the allowlists bound what that reaches and `sealmesh audit` records who
asked, but the daemon cannot verify the router's claim about who is asking.
End-to-end signing by the requester would close that and is not built.

Ticket titles come from the branch slug rather than a tracker's API: no
credential, no rate limit, and `feature/eng-557-qa-rescore-ui` already reads.

## License

Copyright 2026 allocmemory.

| Crates | License |
|---|---|
| `sealmesh` (CLI and daemon), `sealmesh-server` (router), `sealmesh-mcp` | [AGPL-3.0-only](LICENSE-AGPL) |
| `sealmesh-wire`, `sealmesh-proto`, `sealmesh-chat-wire` | [Apache-2.0](LICENSE-APACHE) |

The protocol crates are Apache-2.0 so any tool can speak to Sealmesh; the
programs are AGPL-3.0, so a modified router offered as a service shares its
changes. Contributions are accepted under the [CLA](CLA.md).
