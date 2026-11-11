# Installing Sealmesh

For the person whose machine will be observed. It should take about ten minutes
and should not require asking anyone a question. If it does, that is a bug in
this document — say so.

## What this shares, exactly

Nothing, until someone asks.

The daemon holds a connection to a central server and waits. When a teammate
asks what your agents are doing, that question is routed to your machine, your
machine reads its own Claude Code job files *at that moment*, and answers. There
is no snapshot published on a timer, and nothing about your agents is stored on
the server or anywhere else. Close your laptop and there is nothing left to read.

Per background agent, an answer contains: the job's short id, the repository
slug, a run state (`working` / `idle` / `needs_input` / `done`), the branch name,
a ticket id parsed from that branch, commits ahead of `dev`, a count of
uncommitted files, a completed/total task count, and a pull request number.

**It contains no text written by you or by an agent.** Not your prompts, not the
job name, not task subjects, not the agent's current step. This is structural,
not a policy: the format has no string field that could hold them. Claude Code's
`intent` and `detail` fields *are the raw prompt* — on a live job here, one held
a pasted Slack thread — so they are dropped where the job file is read and have
nowhere to go afterwards.

**It contains no filesystem paths.** A repository is named by its remote slug,
never by where it lives on disk.

**Only repositories you list are visible at all.** A job anywhere else produces
no output — not an empty row, not a redacted one. The fact that it exists is not
answered either.

**You can see who has asked.** Every query your machine answers is recorded
locally: `sealmesh audit`. Queries are answered without prompting you, and
that record is what makes them accountable.

You can see exactly what would be sent before anything is sent, and you can turn
it off at any time with one file. Both are below.

## 1. What you need

- `gh`, logged in. That is the whole credential story: no cloud account, no
  access keys, and nothing for anyone to send you. Membership of your GitHub
  organisation is what grants access.
- A router for that organisation to connect to. Someone runs one; see
  [Running your own router](#running-your-own-router).

## 2. Install

From the first release, any of:

```sh
brew install allocmemory/tap/sealmesh allocmemory/tap/sealmesh-mcp
cargo install --locked sealmesh sealmesh-mcp
# or a prebuilt binary from https://github.com/allocmemory/sealmesh/releases
```

From source, with Rust 1.95 or newer (`rustup toolchain install 1.95.0` — the
repo pins it):

```sh
git clone https://github.com/allocmemory/sealmesh.git
cd sealmesh
cargo install --locked --path crates/sealmesh
cargo install --locked --path crates/sealmesh-mcp
```

That puts `sealmesh` and `sealmesh-mcp` in `~/.cargo/bin`. The path names the crate rather than
the workspace root, which is a virtual manifest and has no binary to install.

**`--locked` is required, not a precaution.** `cargo install` ignores
`Cargo.lock` by default and re-resolves every dependency; that resolve fails
outright, because `gix` brings in `bisync` and every published version of
`bisync` is yanked. If you see a resolution error naming a yanked crate, you
left the flag off.

## 3. Log in

```sh
gh auth login          # skip if `gh auth status` already looks right
gh auth status
```

That is it. The daemon reads the token `gh` already holds.

**The name your rows appear under is derived from this login.** There is no host
field for you to set, and nobody can answer under your name but you. The router
resolves your token to a GitHub login and checks that you are an active member
of the organisation — a pending invitation is not membership, and someone
outside the org cannot connect at all.

One thing to know rather than discover: the daemon sends that token to the
router, which holds it in memory for as long as your daemon stays connected —
joining a room and re-checking your access to it later both use your own
credential. Only that connection's task holds it; it is never written anywhere
and never logged, and it is gone when the connection closes. But it is your `gh`
token with whatever scopes you granted, so the connection has to be encrypted —
that is why the URL below is `https` and not `http`.

## 4. Config

```sh
mkdir -p ~/.config/sealmesh
$EDITOR ~/.config/sealmesh/config.toml
```

```toml
# How often work is measured against a base branch: "3 ahead" means ahead of dev.
integration_branch = "dev"

[server]
url = "https://router.example.com"

[allow]
# Only these repositories are ever answered about. Slugs, exactly as on GitHub.
repos = ["example-org/example-repo"]

# Only jobs whose working directory is under one of these is even looked at.
# Paths are resolved through symlinks before matching, so nothing can escape.
# Use your real checkout location.
paths = ["~/Development"]

# Optional, and these are the defaults. How long a declared agent record is believed
# without a live parent session — see docs/contract.md. Range is 60..=86400.
[declared]
ttl_secs = 900
```

Both allowlists are default-deny. An empty list answers nothing and is a startup
error rather than a silent no-op.

### Ticket rooms (optional)

Ticket rooms are opt-in. List the ticket ID prefixes your branches use and a
checkout on `feature/abc-123-upload` also gets the `ABC-123` ticket room next
to its repo room:

```toml
# Jira-style ABC-123, or GitHub-issue-style GH-123 if you name branches that way.
ticket_prefixes = ["ABC", "GH"]
```

Leave it out (or empty) and no branch is ever read as a ticket: rows carry no
ticket id and a checkout resolves to its repo room alone. The list is also what
keeps `feature/rework-2-uploads` from reading as a ticket "REWORK-2" — only the
prefixes you name count. `sealmesh doctor` prints `ticket rooms: off (no
ticket_prefixes)` when the list is empty.

### Modes

One binary, three ways to run it, picked by `[mesh] mode`. Leave `[mesh]` out
and you get `cloud`, which is everything above: existing configs keep working
unchanged.

```toml
[mesh]
mode = "cloud" # or "std", or "otw"
```

| Mode | Where the router runs | Who you are | Needs |
|---|---|---|---|
| `cloud` (default) | a remote router (`[server] url`) | your GitHub login; access follows your repo permissions | `gh`, `[server]`, `[allow]` |
| `std` | inside `sealmesh daemon`, on this machine | your OS user, proved by a per-boot secret file only you can read | nothing else: `[server]` is ignored and `[allow]` may be empty |
| `otw` | on one of your machines, reached over Tailscale | your Tailscale identity | `[server] url` pointing at that machine's tailnet address; see [otw.md](otw.md) |

`sealmesh doctor` prints the mode. In `std` and `otw` mode you can also name
rooms and say who may join them:

```toml
[[room]]
name = "ops"                     # 1-48 chars of a-z, 0-9 and -
members = ["claude", "grokbot"]
```

A cloud config with a `[[room]]` block is refused at load: a cloud router
decides rooms from GitHub. Both other modes run today: [std.md](std.md) has
the embedded router and `agents.sock` for local bots (`[[agent]] name =
"grokbot"`), with the full config, curl examples and security model, and
[otw.md](otw.md) sets up the tailnet router and the daemons.

### `[events]` (deprecated, off by default)

Events are deprecated and will be removed: the daemon can report its own
transitions on a timer, but nothing on the router consumes them — it counts
each batch and drops it. They are off unless you set:

```toml
[events]
enabled = true      # default false
interval_secs = 120 # range 10..=3600
```

With `enabled = true` the daemon still emits exactly as before and prints one
deprecation warning at start. The allowlists and the off switch gate events the
same way they gate everything else.

## 5. Run each agent in its own worktree

This one is not optional if you want the tool to tell the truth.

Work identity comes from the branch, and git checks out one branch per working
copy. Four agents sharing one checkout all report the same branch and the same
ticket, so everyone reading sees four identical rows — the tool answering its
central question wrong, confidently.

```sh
git worktree add ../example-repo-eng-612 feature/eng-612-upload-history
```

One per background agent. `sealmesh doctor` warns when it catches two sharing a
checkout.

## 5b. Install the Claude Code hook

Optional, and worth it. Without it your sessions show as `working` whenever
they are alive, because a session file only proves a process exists.

```sh
sealmesh install-claude-hook --dry-run   # see what it would change
sealmesh install-claude-hook
```

It edits `~/.claude/settings.json` to run `sealmesh claude-hook` on four events, and
that gives you an honest `idle` when a turn ends and `NEEDS INPUT` the moment a
session puts a question up — read from Claude Code's own events rather than
guessed from how long you have been quiet.

`PreToolUse` is subscribed with matcher `*` and a 5-second timeout, so the hook
runs before every tool call. That's what lets the optional
[guardrail](guardrail.md) deny a call; with no policy file it prints nothing
and costs one `stat`.

**It merges.** Your existing hooks and every setting Claude Code owns are
preserved, and so are the settings file's permissions; running it twice does
nothing. An install from before the guardrail (`PreToolUse` with matcher
`AskUserQuestion`) is replaced by the `*` entry, not duplicated. `--dry-run`
prints the change first if you would rather look.

Two things worth knowing:

- **Restart running sessions** to pick it up. Conductor passes
  `--setting-sources=user,project,local`, so a user-level hook applies inside
  Conductor workspaces too.
- **`claude --bare` skips hooks entirely**, so a `--bare` session is invisible to
  this. It still shows up through its session file, just without the
  `idle` / `needs input` detail.

The hook reads the payload through a typed struct with no field for prose — no
prompt, no assistant message, no transcript path. That is why it is the
`sealmesh` binary and not a shell script: a shell hook that logs its input for
debugging, once, would defeat the whole design. The guardrail reads a tool
call's `tool_input` (the command line, the file path) to match it against your
policy, and keeps none of it: a deny's audit line holds the tool name, the
rule and the policy's reason, never the command.

### Making subagent workers visible

Agent-tool subagents — what an orchestrated wave is made of — run inside the
coordinator's process and write nothing to disk, so nothing can see them by
reading files. Hooks can. `sealmesh claude-hook` handles `SubagentStart` and
`SubagentStop`, giving each worker its own row in its own worktree, parented to
the session that spawned it.

It is **not** installed by default, because it fires on every Agent-tool call in
every session and that is a choice for whoever is running a wave. Add it by hand,
in the same shape as the others:

```json
"SubagentStart": [{"hooks": [{"type": "command", "command": "/path/to/sealmesh claude-hook"}]}],
"SubagentStop":  [{"hooks": [{"type": "command", "command": "/path/to/sealmesh claude-hook"}]}]
```

Use the same absolute path `install-claude-hook` wrote for the other four.

## 6. Check it before it answers anything

```sh
sealmesh doctor
```

This prints your config, the Claude Code roots it found, any declared records,
and **every agent it saw with the reason it was included or hidden**. Working
directories appear here — that is the point of a diagnostic, and it is your own
terminal. None of them are ever answered with.

```
declared records (1 in /Users/shelter/.local/state/sealmesh/agents, ttl 900s)
  aaaa1111  ok         /Users/shelter/.local/state/sealmesh/agents/aaaa1111.json

agents (4 seen: 1 declared, 1 job, 2 session)
  6954de0e  session    hidden     not under an allowed path: /Users/shelter
  57a34c98  session    hidden     example-org/sealmesh is not on allow.repos
  949c7c62  job        PUBLISHED  example-org/example-repo  working  feature/eng-557-qa
  aaaa1111  declared   PUBLISHED  example-org/example-repo  working  feature/eng-612
```

The middle column names the adapter that found each row. `agents (0 seen)` on a
machine that is running agents means an adapter found nothing, which is a bug
worth reporting — it is not the same as an idle machine, and the tally is what
tells the two apart.

Then look at the exact bytes that would leave the machine:

```sh
sealmesh daemon --dry-run
```

This needs no credentials, connects to nothing, and sends nothing. Read it. If
anything in there surprises you, stop and say so before going further.

When you are happy:

```sh
sealmesh daemon
```

Sandbank should see your rows immediately.

## 7. Keep it running

`~/Library/LaunchAgents/dev.sealmesh.daemon.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>dev.sealmesh.daemon</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/shelter/.cargo/bin/sealmesh</string>
    <string>daemon</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/Users/shelter/.cargo/bin:/opt/homebrew/bin:/usr/bin:/bin</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>/Users/shelter/Library/Logs/sealmesh.log</string>
  <key>StandardErrorPath</key>
  <string>/Users/shelter/Library/Logs/sealmesh.log</string>
</dict>
</plist>
```

Replace `/Users/shelter` with your home directory, then:

```sh
launchctl load -w ~/Library/LaunchAgents/dev.sealmesh.daemon.plist
tail -f ~/Library/Logs/sealmesh.log
```

`PATH` matters: the daemon reads its GitHub token by running `gh`, and launchd
starts with a minimal environment that does not include Homebrew.

The log path matters too. `/tmp` is world-writable, launchd creates the file
world-readable, and it follows a symlink that is already sitting at that path —
so a log there is both readable by every account on the machine and a way for a
local process to get the daemon to append to a file of its choosing.
`~/Library/Logs` is neither.

The daemon reconnects on its own. Laptops sleep, wifi drops, and the server is
replaced on every deploy, so a disconnect is normal and the backoff tops out at
thirty seconds.

## 8. The off switch

```sh
touch ~/.config/sealmesh/off      # stop sharing
rm ~/.config/sealmesh/off         # resume
```

It takes effect on the next query — no restart, no uninstall.

While it is on, the daemon stays connected and answers *not sharing*, so your row
reads that rather than going dark. That is deliberate: it means "Shelter turned
it off" and "Shelter's laptop is closed" look different from the other side,
which is the difference that matters when someone is deciding whether to trust
what they see.

The same file turns room chat off too, whatever the config says.

## 8b. Room chat is off by default

Rooms are where agents working on the same ticket, repo or project leave each
other messages. Nothing on this machine joins, posts to or reads a room until
you turn it on in the config:

```toml
[chat]
enabled = true
```

Then restart the daemon. It owns this machine's room memberships and keeps
what arrives in a bounded inbox in memory, and local tools — `sealmesh chat`,
the MCP room tools, the hook — reach it through a Unix socket:

```
~/.local/state/sealmesh/local/daemon.sock     # or $XDG_STATE_HOME/sealmesh/local/
```

Only your user can open it: the directory is `0700`, the socket `0600`, and the
daemon checks the user id of every process that connects. Your GitHub token
never crosses it; the daemon talks to the router with its own connection.

A few things worth knowing:

- **The path has to fit in 103 bytes**, the most a Unix socket path can hold on
  macOS. With a very long home directory or `XDG_STATE_HOME`, the daemon says so
  at startup and runs without room chat; point `XDG_STATE_HOME` somewhere
  shorter.
- **A socket left by a crashed daemon is replaced** on the next start. Anything
  else at that path — a symlink, a file, a socket another daemon is still
  serving — is left alone, and the daemon runs without room chat and says why.
- **Memberships are not saved.** After a reconnect the daemon re-joins its rooms
  on its own; after a restart, rooms are joined again by whatever joined them.
  Messages that arrived while it was down are still in the room's history, which
  is kept for 7 days.
- **Nothing is dropped silently.** When a room's inbox is full the oldest
  message goes, and the loss is counted and reported to whoever reads next.
- With chat off, or with the off file present, every request on the socket is
  answered at once with `disabled`. Turning it off while the daemon runs
  leaves every room it was in.

Two more keys, both off by default and both meaningless while chat is off:

```toml
[chat]
enabled = true
project_rooms = false   # also name the project's own room when resolving a checkout
hook_drain = false      # let the Claude Code hook drain room messages into each turn
```

- **`project_rooms`.** A checkout's repo room (from its `origin`) and, when
  `ticket_prefixes` is set, its ticket room (from its branch) are found for it
  automatically. The project room spans
  every repo in the project, so it is named only when you turn this on. It
  decides only what is *named*: whether you may join any room is still the
  router's call, checked against your GitHub access.
- **`hook_drain`.** On `UserPromptSubmit`, the hook resolves and holds this
  checkout's rooms under the session's own id, drains each one it holds, and —
  only when something is waiting — adds one line per room to
  `hookSpecificOutput.additionalContext`, for example: "3 new messages in the
  ticket room (seq 41–43). Call room_read with after=40 to read them." That is
  all it ever says: no message body, sender or kind reaches this line, ever —
  an agent that wants to read what was said still calls `room_read` itself, on
  its own choice. Bounded to a few rooms and a couple KB of text, under one
  overall deadline of a few hundred milliseconds; a daemon that does not
  answer in time, or that predates the `hold` request this uses (it falls
  back to `resolve` with `join`), costs a quiet turn, never a delayed prompt.
  Off until you want agents to hear their rooms without asking.

Which project a repo belongs to is not in your config, and cannot be. It lives
in the router's environment, and the daemon asks for it on every connection,
cut down to the repos your own GitHub token can read, and again when it is more
than 10 minutes old. A repo GitHub was too slow to confirm is left out of that
cut, so a checkout can find no rooms for up to 10 minutes after the router
restarts. `sealmesh doctor` shows both keys.

## 9. Reading the other machines

```sh
sealmesh who
```

A machine with no daemon connected reads *unreachable*, with no data. It is never
shown as current, and there is no cached last answer anywhere to show.

## 9b. A second machine (same GitHub account)

Install it exactly the same way: steps 2 to 7 again on the other machine, the
same `gh` login, and the same `[server] url`. Nothing else to set up, and
nothing to tell the router.

Both machines then show up in `sealmesh who`, each under your login with its
machine name beside it:

```
santiago@work-laptop — 2 agents, read 1s ago
santiago@home-desktop — 1 agent, read 1s ago
```

The machine name is the computer's hostname, lowercased and cut at the first
dot (`Santiagos-MacBook-Pro.local` reads as `santiagos-macbook-pro`), and
`sealmesh doctor` prints it. To pick your own, set it at the top of the config:

```toml
machine = "work-laptop"   # 1-63 characters of a-z, 0-9 and -
```

Give each machine its own name. The router keeps one connection per machine,
so when a laptop wakes from sleep its new connection takes the place of its
old one; two machines sharing a name would keep taking each other's place.

Everything else stays yours, not the machine's: anything you post in a room
is from your login whichever machine sent it, and the post rate, room token
allowance and turn cap are shared by all your machines.

This needs a router from this release or later. An older router lets only one
of your machines stay connected at a time, so the two take turns appearing in
`who`.

## 10. Who has been looking

```sh
sealmesh audit
```

Every query this machine answered, with who asked and when. One caveat worth
knowing: the asker's name comes from the server, and your daemon has no way to
check it. A compromised server could name someone who never asked. The count is
unconditional — fifty reads show fifty lines regardless.

With room chat on, the same log also records each post of yours the router
refused, with the room and the reason, and every fifteen minutes how many room
messages arrived from each sender in each room. Never a message's text.

## Uninstall

```sh
launchctl unload -w ~/Library/LaunchAgents/dev.sealmesh.daemon.plist
rm ~/Library/LaunchAgents/dev.sealmesh.daemon.plist
rm -rf ~/.config/sealmesh ~/.cache/sealmesh ~/.local/state/sealmesh
cargo uninstall sealmesh
```

Nothing to ask anyone to delete. Stopping the daemon is the whole uninstall as
far as anyone else is concerned — there is no stored snapshot of you to remove.

## If something is wrong

**No rows for my machine.** Run `sealmesh doctor`. It names the gate that
stopped each job.

**Everyone sees the same branch for all my agents.** They share a checkout. See
step 5.

**`allow.paths entry ... does not resolve to a real directory`.** A typo in
`paths`. This is an error on purpose — a path that matches nothing would
otherwise look like a working daemon with no agents.

**Everything says `CI ?`.** `gh` is not on the daemon's `PATH`, or its token
expired. Run `gh auth status`. Nothing else is affected.

**`NOT resolved` under github credential.** `gh` is not logged in, or is not on
the daemon's `PATH`. Run `gh auth status`.

**The server refused this login.** Either the token is not valid, or you are not
an active member of the organisation the router serves. Check `gh auth status`
first, then whether your org invitation is still pending.

---

## Running your own router

On AWS, the Terraform recipe in [deploy/aws](../deploy/aws/README.md) does all of
this for you; see [cloud.md](cloud.md). The rest of this section is for running it
by hand.

One router serves one GitHub organization. It is one process and one SQLite
file: room chat for 7 days and nothing else — no snapshot and no bucket. It
holds live connections and the names of hosts it has seen since it started, and
no snapshot, agent row or `who` answer is written anywhere, so there is
deliberately nowhere for agent activity to accumulate.

**Run exactly one per organization.** The connection registry is in process, so
a second router would hold a second set of daemon sockets and report every
machine attached to the other as unreachable.

### Configure it

`sealmesh-server.toml`:

```toml
github_org = "example-org"           # whose active members may connect
listen = "0.0.0.0:8787"
projects = [
  { id = "example", repos = ["example-org/example-repo", "example-org/other-repo"] },
]

[history]
backend = "sqlite"
path = "/var/lib/sealmesh/history.sqlite"
```

Every key has an environment variable (`SEALMESH_GITHUB_ORG`, `SEALMESH_LISTEN`,
`SEALMESH_PROJECTS` as JSON, `SEALMESH_HISTORY_BACKEND`, `SEALMESH_HISTORY_PATH`,
and the optional limits `SEALMESH_ROOM_TOKENS_PER_HOUR`, `SEALMESH_ROOM_TURN_CAP`,
`SEALMESH_READER_CHECKS_PER_HOUR`, `SEALMESH_ACCESS_CACHE_TTL_SECS`), and the
environment wins over the file. An unknown key is a startup error, not a setting
silently ignored. The org and the history path have no default: a guessed one
would authorize the wrong people, or write an org's coordination history
somewhere nobody is looking.

### Run it

With Docker (the image keeps history in its `/data` volume):

```sh
docker run -d --name sealmesh-server -p 127.0.0.1:8787:8787 \
  -v sealmesh-data:/data \
  -v "$PWD/sealmesh-server.toml:/etc/sealmesh/sealmesh-server.toml:ro" \
  -e SEALMESH_CONFIG=/etc/sealmesh/sealmesh-server.toml \
  -e SEALMESH_TLS_TERMINATED_BY=caddy \
  ghcr.io/allocmemory/sealmesh-server:latest
docker exec sealmesh-server sealmesh-server healthcheck
```

Inside a container the router binds `0.0.0.0`, so it refuses to start until
you say what terminates TLS in front of it (`SEALMESH_TLS_TERMINATED_BY`, see
below) — publish the port on loopback only and put that proxy in front.

Or as a systemd service, with the binary from the releases page:

```ini
# /etc/systemd/system/sealmesh-server.service
[Unit]
Description=Sealmesh router
After=network-online.target

[Service]
ExecStart=/usr/local/bin/sealmesh-server --config /etc/sealmesh/sealmesh-server.toml
DynamicUser=yes
StateDirectory=sealmesh
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

```sh
sudo systemctl enable --now sealmesh-server
sealmesh-server --config /etc/sealmesh/sealmesh-server.toml healthcheck
```

### Put TLS in front of it

The handshake carries each person's GitHub token, so anything that is not
loopback must be `https`. Terminate TLS in front of the router (Caddy, nginx, a
cloud load balancer) and tell the router so: `tls_terminated_by = "caddy"` (or
`SEALMESH_TLS_TERMINATED_BY`). Without it, the router refuses to listen in
plaintext on anything but loopback.

Keep the proxy's idle timeout above 30 seconds: daemons heartbeat every 30
seconds to stay connected. If every machine drops about a minute after
connecting, that is a 60-second idle timer, not the network.

Then each person's `[server] url` is your router's `https://` address.

### DynamoDB instead of SQLite

A build with `--features dynamodb` can keep history in a DynamoDB table instead:
`[history] backend = "dynamodb"` and `table = "..."` (or `DYNAMODB_ROOM_HISTORY`),
partition key `room` (string), sort key `seq` (number), with a TTL on
`expires_at`. Credentials come from the AWS SDK's default chain; nothing about
them is configured here.

### Onboarding

Add someone to the GitHub organization and point them at this document. There
is no account to grant and no key to send. Removing someone is removing them
from the org.
