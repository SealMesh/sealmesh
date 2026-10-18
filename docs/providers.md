# Adding a tool

sealmesh reads Claude Code natively. Any other agentic tool — Codex, Qwen,
Ollama, something you wrote yourself — gets in one of two ways, and the right
one depends on whether the tool already writes state to disk.

There is a third document next to this one. [`contract.md`](contract.md) is the
**push** channel: your tool tells sealmesh what it is doing. This file is the
**pull** channel: sealmesh reads files your tool already writes. Neither is
better; pick the one that matches what you have.

## Option 1 — your tool can tell us (push)

If you can run a command when your agent starts, changes state, and stops, use
`sealmesh declare`. Nothing to configure, no rebuild, and it works today:

```sh
sealmesh declare --id "$(echo -n "$RUN" | shasum | cut -c1-8)" \
                   --tool codex \
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
| Claude Code session files | 100 |

A config-added provider sits below `declare` on purpose: a tool that named
itself knows more than a directory we inferred things from.

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
