<!-- Purpose: the writer-facing declared-agent contract. This is the document an
     engineer writing any tool reads to make it visible to sealmesh, so it must
     be complete without reading any Rust.
     Maintained by: humans, alongside `crates/sealmesh/src/collect/declared.rs`.
     A field added to one and not the other is a bug in whichever moved second. -->

# The declared-agent contract

**How any tool tells sealmesh that an agent is running, without sealmesh
knowing anything about that tool.**

sealmesh reads Claude Code's own files directly, because it can. Nothing else
gets that treatment. Everything else — an orchestrator, an editor hook, a CI
runner, an agent framework that does not exist yet — writes one small JSON file
per agent, and sealmesh reads it the same way it reads everything else: through
the same two allowlists, into the same row.

You do not need to depend on sealmesh, link against it, or run it. Write the
file, or shell out to `sealmesh declare` if the binary happens to be on
`PATH`. If it is not, that is a no-op and your tool carries on.

## The one rule

**No field here may hold text a person or a model wrote.**

This is not a guideline about being careful. Every field below is a number, a
timestamp, a word from a closed list, or a string with a stated character set
and length limit. There is no field a sentence fits in.

The reason is specific. sealmesh was built against a machine where a live
agent's stored prompt held a pasted private Slack thread naming a third party.
Prompts, task descriptions, session names, and "waiting for" strings are all
routinely full of things nobody meant to broadcast to twenty colleagues. A
format that *could* carry them would eventually carry them, no matter who
promised to strip what.

So a conforming writer never has to decide what is safe to send. There is
nowhere to put the unsafe thing.

**A record with a field not listed below is rejected — the whole record, not
the field.** That is deliberate. Dropping an unknown field silently would make
the format widen by accident; refusing the record makes it widen only on
purpose.

## Where the file goes

```
$XDG_STATE_HOME/sealmesh/agents/<id>.json
```

`$XDG_STATE_HOME` defaults to `~/.local/state`, so on a normal machine:

```
~/.local/state/sealmesh/agents/1a2b3c4d.json
```

One file per agent. The filename is `<id>.json`, and `<id>` must equal the
record's own `id` field — a file whose name and contents disagree is rejected.

**Write it atomically.** Write to a temporary file *in the same directory*, then
rename over the target. A reader can arrive mid-write at any moment, and a
half-written file reads as a corrupt record rather than as the previous one.

```sh
tmp="$dir/.$id.json.tmp.$$"
printf '%s' "$json" > "$tmp" && mv -f "$tmp" "$dir/$id.json"
```

`mv` within one filesystem is atomic. Writing to `/tmp` and moving across
devices is not — that is a copy, and a copy can be read halfway through.

**Delete the file when the agent stops.** A record left behind claims an agent
that is not running. There is a TTL as a backstop, but a backstop is not a
substitute: between the agent stopping and the TTL expiring, your record is
lying.

## The fields

| Field | Type | Required | What it may hold |
|---|---|---|---|
| `id` | string | yes | 4–64 characters of `[a-f0-9-]`. Lowercase hex and hyphens, nothing else. Use the first 8 characters of a session UUID if you have one. |
| `worktree` | string | yes | An **absolute** path to the agent's working directory. **Local only** — see below. |
| `run_state` | string | yes | Exactly one of `working`, `idle`, `needs_input`, `done`. |
| `started_at` | string | yes | RFC 3339 timestamp, e.g. `2026-08-24T11:41:03.591Z`. **Do not move it on a refresh** — see below. |
| `updated_at` | string | yes | RFC 3339 timestamp. Refresh it as the agent works — the TTL is measured from here. |
| `branch` | string | no | A git branch name. **Wins over the branch sealmesh reads from the worktree** — see below. Omit it and the worktree's `HEAD` is used instead. Must pass git's `check-ref-format` and may not begin with `-`. |
| `pr` | number | no | A pull request **number**, 1–999999. Not a URL. |
| `tasks` | object | no | `{"completed": <n>, "total": <n>}`, both 0–65535, `completed ≤ total`, and `total ≥ 1`. Omit it entirely if there is no task list — see "no data is not zero". |
| `parent` | string | no | The id of a session this agent runs under, as reported by whichever tool owns it. 1–64 characters of `[A-Za-z0-9_-]`. See "liveness". |
| `tool` | string | no | What tool you are, e.g. `codex`. 1–32 characters of `[a-z0-9-]`, starting alphanumeric. Omit it and the row reads `declared`. This is the one thing you are taken at your word about; everything else is validated. |

Anything else is a rejection.

### A complete record

```json
{
  "id": "1a2b3c4d",
  "worktree": "/Users/dart/work/checkout-service",
  "run_state": "working",
  "started_at": "2026-08-24T11:41:03.591Z",
  "updated_at": "2026-08-24T12:07:19.004Z",
  "branch": "feature/ABC-412-retry-budget",
  "pr": 627,
  "tasks": { "completed": 3, "total": 8 },
  "parent": "a04e3d0f-d94b-4d7e-9f02-5b09827a67f9"
}
```

### The minimum record

```json
{
  "id": "1a2b3c4d",
  "worktree": "/Users/dart/work/checkout-service",
  "run_state": "working",
  "started_at": "2026-08-24T11:41:03.591Z",
  "updated_at": "2026-08-24T11:41:03.591Z"
}
```

Everything else sealmesh works out for itself: the repository from the
worktree's git remote, the branch from its HEAD, the ticket from the branch, how
far ahead of the integration branch it is, and how many files are dirty.

Declare a `branch` only when you know one the worktree does not have — that is
the one field where your answer replaces git's rather than filling a gap.

## Notes on individual fields

### `worktree` never leaves the machine

It is in the record because sealmesh needs it, and it is used for exactly two
things: deciding whether this agent is inside a directory the machine's owner
allowlisted, and resolving the git repository it belongs to.

The path itself is **never sent anywhere**. What goes on the wire is the
repository slug — `acme/checkout-service` — because `~/clients/nda-holdings`
leaks a client name in the directory name alone.

A worktree outside the machine's allowed paths produces nothing at all. Not an
error, not a hidden row: sealmesh does not report that an agent it may not
describe exists, because "Dart has an agent you cannot see" is itself
information about a client.

### `run_state` is four words and no others

- `working` — actively doing something.
- `idle` — alive, not currently working. Between turns.
- `needs_input` — blocked on a person. Report this only when it is true; it is
  the state the whole tool exists to surface, and a writer that guesses at it
  from silence makes every row less trustworthy.
- `done` — finished. Prefer deleting the record; use `done` when you want the
  final state visible for a moment first.

There is no `error`, and that is a decision rather than an omission. What failed
is prose, and this format has no room for prose — a bare "error" with no way to
say what went wrong is a row that makes a reader open a terminal, which is what
they were going to do anyway.

### `started_at` is the start, not the last time you said so

`updated_at` moves every refresh. `started_at` must not.

This is the one field where refreshing is easy to get wrong, because the
obvious implementation — stamp both from the clock on every write — is wrong in
a way nothing complains about. The agent's age is then always about zero, and a
row that says a three-hour agent started a moment ago is worse than a row with
no age at all.

If you shell out to `sealmesh declare`, this is handled: it reads the start
already on disk and carries it forward, so a refresh moves only `updated_at`.
If you write the file yourself, keep the value you first wrote.

sealmesh defends against it where it can — when another source describes the
same agent, the earlier of the two starts is the one that renders — but it can
only do that when there *is* another source, and for most writers there is not.

### `tasks`: no data is not zero

Omit `tasks` when there is no task list. Do not send `{"completed": 0, "total":
0}`.

An agent with no task list and an agent that has completed none of eight are
different facts, and they render differently: a dash versus an empty progress
bar. `total: 0` is rejected outright — it is a division by zero in the first
renderer that computes a percentage, and that is a crash a record on someone
else's laptop can cause.

### `branch` overrides the worktree, and only `branch` does

Every other fact about the checkout — the repository, how far ahead it is, how
many files are dirty — comes from git reading the worktree, and nothing you
declare can change them. `branch` is the exception, and it is deliberate.

The two are not the same kind of claim. You naming a branch is first-hand
knowledge: you launched the agent, you chose the branch. Reading `HEAD` is an
inference, and it is wrong whenever the worktree's branch belongs to a harness
rather than to the work. An agent isolated in a worktree is the case that forced
this — it sits on a synthetic `worktree-agent-<id>` branch while its real
feature branch exists only in the work order that launched it. Git answers
confidently and answers the wrong question, and because the ticket is parsed
from the branch, the row loses its ticket too.

So: omit `branch` and you get the worktree's, which is right for most writers.
Declare one and it is used. If you declare a branch that disagrees with a
worktree whose `HEAD` was fine, yours still wins — that is the cost of the rule,
and it is the writer's job not to lie.

### `pr` is a number

Not a URL. A number cannot point at a different host, and the repository is
already known from the worktree, so a number is all the information a URL would
have carried.

## Liveness — how sealmesh knows your record is still true

A record can go stale. Your process crashed, the laptop slept, someone
`kill -9`'d the wave. Two mechanisms, and you should understand which one
applies to you.

**The TTL.** A record older than `updated_at + 900 seconds` is dropped, and
`sealmesh doctor` marks it `stale` rather than pretending it is gone. Fifteen
minutes is a compromise: short enough that a crashed writer stops lying within a
coffee break, long enough that a genuinely slow tool call does not read as
stale. The machine's owner can change it with `[declared] ttl_secs` in their
config.

**`parent`, if you have one.** If your agent runs under a Claude Code session,
put that session's id in `parent`. sealmesh then governs your record by
whether that *process* is alive — checked against the pid and its start time, so
pid reuse cannot resurrect it — instead of by the TTL. That is strictly better:
it is immediate rather than up to fifteen minutes late.

`parent` is optional on purpose. Requiring it would have made a Claude Code
session the only thing that can be alive, which would exclude a CI runner, a
different agent framework, or anything else — and a contract that only one
vendor's children can satisfy is not a contract.

**sealmesh never infers liveness any other way.** It does not scan the process
table for something that looks like yours, and it does not decide you are gone
because you have been quiet. Both would be guesses presented as facts.

## Using `sealmesh declare` instead

If the binary is on `PATH`, this is the same thing without writing JSON, and it
validates every value before anything reaches disk:

```sh
sealmesh declare --id 1a2b3c4d \
                   --worktree /Users/dart/work/checkout-service \
                   --state working \
                   --tasks 3/8 \
                   --pr 627 \
                   --parent a04e3d0f-d94b-4d7e-9f02-5b09827a67f9

# ... as the agent works, to push the TTL out and change state
sealmesh declare --id 1a2b3c4d --worktree ... --state needs_input

# when it stops
sealmesh declare --id 1a2b3c4d --end
```

It writes atomically, so you do not have to get the rename right, and it exits
`0` with one line on stderr when the state directory is missing or unwritable —
a writer's own flow must not break because sealmesh's directory is not there.

**Gate on the binary existing, not on it succeeding:**

```sh
command -v sealmesh >/dev/null && sealmesh declare --id "$id" ... || true
```

That is the whole integration. Your tool keeps working on a machine that has
never heard of sealmesh.

## A worked example: the Claude Code hook

`sealmesh claude-hook` is the first writer against this contract, and it is written
the way an external one should be. It reads Claude Code's hook payload through a
struct with no field for the payload's prose to land in, maps four events onto
three of the four `run_state` words plus a delete, and calls the same
`declare` path this document describes.

Two of its choices are worth copying:

- **It always exits 0.** A `UserPromptSubmit` hook that fails blocks the person
  typing. Whatever your writer is embedded in, decide what your failure mode
  costs the host and make it the cheap one.
- **It declares subagents with `parent` set to the session that spawned them.**
  When the coordinator dies, its workers stop being rows immediately rather than
  fifteen minutes later.

## What a reader actually sees

Your declared agent becomes one row, indistinguishable from any other:

```
dart
  1a2b3c4d  acme/checkout-service   working   ABC-412  3/8  PR #627  22m
```

No path, no prompt, no tool name, no mention of what wrote the record.

## What a writer can and cannot do

Worth stating plainly, because this is a surface other processes write *for*
sealmesh, and that is new.

**A writer can lie about state.** A local process can declare an agent that does
not exist, or claim `working` when it is idle. Nothing here prevents that, and
nothing tries to: every process in this list already runs as the machine's owner
and could do worse things more directly.

**A writer cannot widen the format.** It cannot smuggle a sentence, a path, or
an unknown field onto the wire, because the reader refuses the *record* rather
than dropping the *field*. And the allowlist gates still bound what a declared
`worktree` can reach — declaring one outside the owner's allowed paths produces
nothing.

That is the trade the contract makes: it trusts local processes about facts, and
trusts none of them about format.

## See also

- [`providers.md`](providers.md) — the pull channel: how adapters read files a
  tool already writes, and reach the same gate as declared records.
- [`install.md`](install.md) — running the daemon on a machine to be observed.
- [`security-rules.md`](security-rules.md) — the rules the collector holds.
