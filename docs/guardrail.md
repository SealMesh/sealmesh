# Guardrail

A local policy file of deny rules that the Claude Code hook checks before
every tool call. When a rule matches, Claude Code blocks the call and tells
the agent why. Every deny lands in `sealmesh audit`.

It's for the ordinary mistakes you'd rather never see: a force-push to
`main`, an agent reading `~/.ssh`, a `curl` to a host nobody vetted.

**It's a guardrail, not a sandbox.** An agent determined to get past a regex
can spell a shell command so the regex never sees it. What this gives you is
a stop on the common path, a reason the agent reads, and a record. For hard
isolation, run agents in a container or a VM.

## How it works

1. `sealmesh install-claude-hook` subscribes `PreToolUse` with matcher `*`, so
   `sealmesh claude-hook` runs before every tool call, with a 5-second timeout.
2. The hook reads `/etc/sealmesh/policy.toml`. No file means no guardrail:
   nothing is printed and Claude Code decides as it always would.
3. With a policy loaded, the hook checks the call against the built-in rule
   and then each `[[deny]]` rule, in order. The first match wins and the hook
   prints:

   ```json
   {"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"<reason>"}}
   ```

   No match prints nothing. The hook always exits 0.

`AskUserQuestion` is never denied. It's how an agent asks you something, and
the same hook uses it to mark the session `NEEDS INPUT`.

## Install the policy

The file is owned by root and edited with `sudo`. Agents run as you, so they
can read it but can't change it.

```sh
sudo mkdir -p /etc/sealmesh
sudo $EDITOR /etc/sealmesh/policy.toml
sudo chown root /etc/sealmesh/policy.toml
sudo chmod 0644 /etc/sealmesh/policy.toml
sealmesh doctor | grep guardrail     # guardrail         on (5 rules)
```

If you installed the hook before the guardrail existed, run
`sealmesh install-claude-hook` again. It replaces the old
`PreToolUse(AskUserQuestion)` entry with `PreToolUse(*)` rather than adding a
second one. Restart running sessions to pick it up.

The path is fixed. There's no environment variable or config key to move it,
because anything you can set, an agent running as you can set too.

## Policy format

```toml
[[deny]]
tool = "Bash"                      # or "Edit", "Write", "*", or a list: ["Read", "Edit"]
command = 'regex'                  # Bash only: matched against the command line
reason = "What the agent is told." # required

[[deny]]
tool = ["Read", "Edit", "Write"]
path = "glob"                      # file tools: matched against the path
unless = "glob"                    # optional exception, same kind as command/path
reason = "..."
```

- **`tool`**: a tool name exactly as Claude Code spells it (`Bash`, `Read`,
  `Edit`, `MultiEdit`, `Write`, `NotebookEdit`, `Grep`, `Glob`, `WebFetch`,
  `mcp__server__tool`, ...), `"*"` for every tool, or a list of names.
- **`command`**: a regex over Bash's `tool_input.command`. Unanchored, so it
  matches anywhere in the line. `.` doesn't cross newlines unless you start
  the pattern with `(?s)`. No lookaround: use `unless` for exceptions.
- **`path`**: a glob over the file a tool names (`file_path`,
  `notebook_path`, or `path`). Relative paths are resolved against the
  session's working directory, `.` and `..` are folded away, and symlinks are
  followed, so a link to a denied file is denied too.
  - `*` matches within one path segment, `?` one character, `**` across
    segments. `/a/**` matches `/a` and everything under it.
  - A leading `~/` is your home directory.
  - A glob with no `/` matches the file name alone, like a `.gitignore` line:
    `.env*` matches `.env` anywhere.
  - Case is ignored, because macOS ignores it.
- **`unless`**: an exception to `command` (a regex) or `path` (a glob). The
  call is denied when `command`/`path` matches and `unless` doesn't.
- **`reason`**: what the agent reads. Make it say what to do instead.
- A rule with only `tool` denies that tool outright.

Unknown keys are an error, so a typo can't silently turn a rule off.

Limits: 64 KiB of file, 256 rules, 1,024 bytes per pattern, and regexes are
compiled with a size and nesting limit. A file over any of them is ignored
(see below).

### Example: no force-push to `main`

```toml
[[deny]]
tool = "Bash"
command = '\bgit\b.*\bpush\b.*((--force|\s-f\b).*[\s:+]main(\s|$)|[\s:+]main\s(.*\s)?(--force|-f\b)|\s\+(\S*:)?main(\s|$))'
reason = "Force-pushing main is off limits. Push a branch and open a PR."
```

Catches `git push --force origin main`, `git push -f origin main`,
`git push origin main --force-with-lease` and `git push origin +main`. Leaves
`git push origin main` and `git push --force origin feature/x` alone.

### Example: no reading `~/.ssh` or `.env`

```toml
[[deny]]
tool = ["Read", "Edit", "Write", "MultiEdit", "Grep", "Glob"]
path = "~/.ssh/**"
reason = "SSH keys and config are off limits to agents."

[[deny]]
tool = "*"
path = ".env*"
unless = ".env.example"
reason = "Secrets in .env files stay out of agent context. .env.example is fine."

[[deny]]
tool = "Bash"
command = '\.ssh/|(^|[\s/="])\.env(\.[\w.-]+)?($|[\s";|&)])'
reason = "Reading ~/.ssh or .env files from the shell is off limits too."
```

The file tools and the shell are separate rules: a `path` glob never sees a
Bash command, and a `command` regex never sees a `Read`.

### Example: no `curl` to unknown hosts

```toml
[[deny]]
tool = "Bash"
command = '\b(curl|wget)\b'
unless = '^\s*(curl|wget)(\s+-[\w-]+)*\s+"?https://(api\.github\.com|registry\.npmjs\.org|static\.rust-lang\.org)(/[^\s;&|<>`$()"]*)?"?\s*$'
reason = "curl and wget may only reach api.github.com, registry.npmjs.org and static.rust-lang.org."
```

`unless` has to match the whole command, so `curl https://api.github.com/ &&
curl https://evil.example` is still denied, and so is
`https://api.github.com.evil.example`.

## The built-in rule

Whenever a policy is loaded, the hook also denies:

- any `Edit`, `Write`, `MultiEdit` or `NotebookEdit` of the policy file,
  under any spelling or symlink that resolves to it, and
- any Bash command that mentions `/etc/sealmesh` or `sealmesh/policy.toml`.

The reason given is "the guardrail policy can't be changed by an agent". You
edit the file yourself, with `sudo`, outside an agent session.

## When the policy is ignored: fail open, and say so

If `/etc/sealmesh/policy.toml` exists but:

- isn't owned by root,
- is writable by group or others (`mode & 0o022 != 0`),
- isn't a regular file, or is over 64 KiB, or
- doesn't parse (bad TOML, an unknown key, a bad regex, a rule over a limit),

the hook treats it as missing and **allows** the call. It prints a warning to
stderr, writes a `policy ignored` line to the audit log (once an hour per
distinct problem, not once per tool call), and `sealmesh doctor` shows:

```text
guardrail         WARNING: policy ignored: /etc/sealmesh/policy.toml is writable by group or others (mode 0664)
```

Failing open is deliberate. A broken policy that blocked every call would
wedge every agent on the machine, including the session you'd use to fix it.
The cost is that a broken policy protects nothing until it's fixed, which is
why it's loud in three places.

One case fails closed: a tool call too large for the hook to read (over
8 MiB, almost always a `Write` of a huge file) is denied while a valid policy
is loaded, so padding a call isn't a way past a rule.

## Audit

Every deny and every ignored-policy warning goes to
`$XDG_STATE_HOME/sealmesh/audit.jsonl` (default
`~/.local/state/sealmesh/audit.jsonl`), and `sealmesh audit` shows them:

```text
2026-11-10T12:00:00Z  guardrail  denied Bash (deny #1) session a04e3d0f: Force-pushing main is off limits. Push a branch and open a PR.
2026-11-10T12:05:00Z  guardrail  WARNING: policy ignored, tool calls allowed: /etc/sealmesh/policy.toml is not owned by root
```

A deny line records the tool, which rule (`built-in`, `deny #N` counting
from 1, or `oversized`), the session's short id, and the policy's reason. It
never records the command or the file contents the agent sent.

The audit log is your own file, so treat it as a record for you, not
tamper-proof evidence.
