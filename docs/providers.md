# Adding a tool

sealmesh reads Claude Code and Codex natively (see "Native providers" below).
Any other agentic tool — Qwen, Ollama, something you wrote yourself — gets in
one of two ways, and the right one depends on whether the tool already writes
state to disk.

There is a third document next to this one. [`contract.md`](contract.md) is the
**push** channel: your tool tells sealmesh what it is doing. This file is the
**pull** channel: sealmesh reads files your tool already writes. Neither is
better; pick the one that matches what you have.

## Option 1 — your tool can tell us (push)

If you can run a command when your agent starts, changes state, and stops, use
`sealmesh declare`. Nothing to configure, no rebuild, and it works today:

```sh
sealmesh declare --id "$(echo -n "$RUN" | shasum | cut -c1-8)" \
                   --tool aider \
                   --worktree "$PWD" \
                   --state working

# ...and when it exits, in a trap so a crash still fires it:
sealmesh declare --id "$ID" --end
```

Read [`contract.md`](contract.md) for the field rules. Two things people get
wrong: `--worktree` must be the **git repository** the agent is working in, not
the shell's cwd — a directory that does not resolve to a repo is dropped by the
gate. And if nothing calls `--end`, the record keeps claiming an agent is
running until the TTL expires, so refresh it periodically rather than once.

If your tool has a hook system, point the hook at `sealmesh declare`. That is
all `sealmesh claude-hook` is, and it is why there is no second hook
implementation to write.

## Option 2 — your tool already writes files (pull)

If the tool writes one JSON file per run, describe where and sealmesh reads
them. No code, no PR, no release.

Say a tool writes `~/.qwen/runs/<id>.json` like this:

```json
{
  "dir": "/Users/dart/work/checkout-service",
  "status": "running",
  "started": "2026-09-09T14:02:11Z",
  "git": { "branch": "feat/checkout" },
  "pull_request": 412
}
```

Then in `~/.config/sealmesh/config.toml`:

```toml
[[provider]]
name   = "qwen"                # becomes the tool label on every row
root   = "/Users/dart/.qwen/runs"
layout = "file_per_agent"      # the only shape v1 reads
priority = 150                 # optional; see "who wins" below

[provider.time]
format = "rfc3339"             # or "unix_ms", "unix_s"

[provider.fields]
work_dir   = "dir"             # required
run_state  = "status"          # required
started_at = "started"         # required
updated_at = "started"         # optional, defaults to started_at
branch     = "git.branch"      # optional, dotted path into the object
pr         = "pull_request"    # optional

[provider.run_state]           # your words on the left of the list
working     = ["running", "busy"]
idle        = ["sleeping"]
needs_input = ["waiting", "blocked"]
done        = ["finished", "exited"]

[provider.liveness]
kind     = "ttl"
ttl_secs = 900
```

Run `sealmesh doctor` after editing. It parses the config without connecting
to anything, so it is the cheap way to check a block before restarting the
daemon. **The daemon reads config once at startup and never reloads it** —
editing the file under a running daemon does nothing, and nothing tells you so.

### What the config can and cannot do

It says **where to look**. It never says **what is allowed**.

Every value it pulls out goes through the same validation a native provider's
would: a branch that fails git's ref rules is dropped, a PR number outside
1–999999 is dropped, a status word you did not map reads `unknown` rather than
being guessed at. A record missing a required field is skipped and every other
record still publishes — a schema change at the vendor costs you rows from one
tool, never the whole snapshot.

There are no predicates. No `if`, no comparison, no regex, no arithmetic, no
array indexing. `run_state` is the only many-to-one mapping and it is a fixed
set of four keys. This is deliberate: a config file that could express
conditions would eventually be asked to express "publish this one anyway", and
the one rule this tool actually rests on is that no source decides for itself
what gets published.

Your `allow.paths` and `allow.repos` gate a config-added provider exactly like a
built-in one. There is no way to write a block that escapes them.

Unknown keys are refused. A typo makes the daemon **refuse to start** rather
than silently ignore a setting — a daemon that starts and reports nothing looks
identical to a machine with nothing running, which is the failure mode worth
avoiding.

### About the ids

`who` will show an id like `a3f1c2d8` that appears nowhere in your tool's own
output. That is on purpose.

sealmesh ids are 4–64 characters of `[a-f0-9-]`, so a run named
`refactor-the-auth-thing` cannot be one. Passing the raw value through would be
worse than inconvenient: some tools name a run after the prompt that started it,
and that would put a sentence on the wire through a field that looks like an
identifier. So the id is always a hash of `(tool name, your id)` — stable across
scans, and with no way to turn it off.

`sealmesh doctor` prints your original id beside the derived one, which is
where to look when you need to map a row back to a file.

### Who wins when two providers see the same agent

Each provider has a priority. Higher wins; ties break on which record was
updated most recently, then on provider name so the result never depends on
config ordering.

| Provider | Priority |
|---|---|
| Claude Code job files | 300 |
| `sealmesh declare` records | 200 |
| **`[[provider]]` blocks** | **150** (default, override with `priority`) |
| Codex rollouts | 110 |
| Claude Code session files | 100 |

A config-added provider sits below `declare` on purpose: a tool that named
itself knows more than a directory we inferred things from.

A `[[provider]]` block named `codex` is **skipped**, not ranked: the native
Codex reader owns that name. Priority only settles two views of the same id,
and the two readers would derive different ids for one session (a uuid suffix
versus a hash), so running both would show every Codex session twice.
`sealmesh doctor` names a skipped block. The config still loads, so a block
written before Codex was native does not stop the daemon.

## Native providers

### Claude Code

Read from every `~/.claude*` directory (plus `SEALMESH_CLAUDE_CONFIG_DIR` and
`[claude] extra_roots`): `jobs/<id>/state.json` and `sessions/<pid>.json`, with
the session's pid checked against the process table.

### Codex

Read from `$CODEX_HOME`, or `~/.codex` when it is unset — the same rule Codex
uses. The layout relied on, from Codex's own rollout writer
(`codex-rs/rollout` in [openai/codex](https://github.com/openai/codex)):

```text
$CODEX_HOME/sessions/YYYY/MM/DD/rollout-YYYY-MM-DDThh-mm-ss-<thread uuid>.jsonl
```

One JSONL file per thread, `{"timestamp", "type", "payload"}` per line. The
date directories are local time; a reverted thread's file name carries a
second uuid after an `_`, and the thread uuid stays the identity. sealmesh
reads, and only reads:

| Record | Fields used | For |
|---|---|---|
| first line, `type = "session_meta"` | `payload.timestamp`, `payload.cwd`, `payload.source`, `payload.parent_thread_id` | start time, launch directory, skipping subagent/internal threads |
| `type = "turn_context"` | `payload.cwd` | the directory the latest turn ran in (wins over the launch directory) |
| `type = "event_msg"` | `payload.type` | `task_started` → working; `task_complete` / `turn_aborted` → idle; none in view → unknown |

Liveness is the file's mtime: a rollout written to within the last **15
minutes** is a live session. Codex records no pid and persists no session-end
event, so there is nothing firmer to check; an idle Codex left open longer than
that drops out of `who` until its next turn. Codex's per-thread writer lock
under `thread-writer-locks/` is deliberately not probed — taking even a shared
lock on it could make a starting Codex refuse its own thread.

Bounds: only day directories from the last 8 days are listed, at most the 32
most recent live rollouts are opened, the first line is read up to 256 KiB and
the last 128 KiB of the file for the rest — never the middle. A turn whose
output alone is larger than that window reads as `unknown`. Compressed
`.jsonl.zst` rollouts (Codex compresses files a week cold) and
`archived_sessions/` are ignored.

Never read into memory: message text, tool output, instructions, the git remote
or branch Codex recorded at start. Each line is parsed into a struct with no
field for any of them, and the repo and branch come from the same git lookup on
the working directory that every other row uses. The row id is the last eight
hex digits of the thread uuid — Codex mints UUIDv7s, whose leading digits are a
timestamp shared by sessions started in the same minute.

## Option 3 — write a native provider

Worth it when the tool has structure a config file flattens: its own liveness
signal, task progress, subagents, a session concept other tools can attach to.

Implement `Provider` in `crates/sealmesh/src/collect/providers.rs` — an id, a
priority, a `scan`, and optionally `live_sessions` if your tool has sessions
another tool's records can name as a parent. The trait is four methods and the
funnel does not change. Open a PR.

## What v1 deliberately does not do

Each of these is a real request that was left out rather than forgotten:

- **One file listing many agents.** Wanted for Ollama. Needs an array shape and
  a rule for where each element's id comes from.
- **Computed fields** — arithmetic, string joining, defaults from other fields.
  This is the edge of a scripting language and the answer is a native provider.
- **Shelling out to a vendor CLI.** That is a native provider by definition: it
  runs code, so it needs review.
- **`kind = "pid"` liveness.** The shape is reserved in the config; only `ttl`
  is implemented.
