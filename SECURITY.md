# Security policy

Sealmesh reads what coding agents are doing on people's own machines and shows
it to their teammates. The thing it handles is not a database — it is what
people are working on, including work they have not shared. Treat a report
about it as urgent even when the bug looks small.

## Reporting a vulnerability

**Report it privately through GitHub:** the repository's **Security** tab →
**Report a vulnerability**. Do not open a public issue, and do not describe it
in a pull request title.

Include what you can:

- what you can do that you should not be able to;
- the smallest sequence that shows it;
- which component — the CLI and daemon (`crates/sealmesh`), the router
  (`crates/sealmesh-server`), the MCP server (`crates/sealmesh-mcp`), the wire
  format (`crates/sealmesh-wire`), the frame protocol (`crates/sealmesh-proto`),
  or the build and release (`Dockerfile`, `.github/workflows/`);
- the commit or version you found it on.

You will get an acknowledgement within two working days and an assessment
within five. If the finding is confirmed you will hear the fix and when it
ships. There is no bounty and no SLA beyond those two numbers.

## Scope

This repository, the binaries and images it releases, and a router run from
them.

**In scope, and the four things most worth attacking:**

- Anything that lets one person's activity reach someone it should not — a
  bypass of the repo or path allowlist, or free text escaping into the wire
  format.
- Anything that lets a client be attributed to an identity that is not its own.
- Anything that makes a machine answer a query it should have refused, or
  answer more often than its own rate limit allows.
- Anything that gets code or credentials out of the build and release
  pipeline.

**Out of scope:**

- The GitHub and coding-agent products this depends on. Report those to their
  vendors.
- A machine already being compromised. Everything the collector reads is
  readable by that account anyway; Sealmesh is not a boundary there.
- Anything requiring a person to deliberately misconfigure their own
  `allow.paths` to include a tree they did not mean to share. That is a
  documentation problem, and we would still like to hear about it.

## What this system is built to protect, and what it is not

[`docs/security-rules.md`](docs/security-rules.md) has the reasoning and the
numbered rules. The short version, so a report can be aimed properly.

### Held

**No field on the wire may hold text a person wrote.** Claude Code's
`state.json` carries the raw prompt in `intent` and `detail`. Every field on
`Snapshot` and `Agent` is an enum, a number, a timestamp, or a newtype whose
constructor enforces a charset and a length, so a prompt cannot be *assigned*
to any of them. A way to get free text onto the wire is the highest-severity
finding this project has.

**Identity comes from the credential.** The host a connection speaks for is
derived from the GitHub identity its token resolves to — never from a path, a
body, or config. There is no `host` field in the config file, and a config that
sets one is refused rather than ignored.

**A panic is a denial of service.** Input arrives from a process on someone
else's machine, so `unsafe_code` is forbidden and `unwrap`, `expect`, `panic`,
and slice indexing are denied outside tests. A crash another machine can
trigger is a bug, not a robustness nit.

**Nothing about agents is stored centrally.** The router is a connection
registry. There is no snapshot store, no cache, and no last-known answer, so a
machine that disconnects leaves nothing behind. Room chat is the one thing kept
at rest, for 7 days.

### Known and not yet held

These are recorded rather than hidden. A report that finds one of them is
appreciated but is not news; a report that finds them to be *worse* than
described is.

1. **A compromised router is a remote read trigger.** Whoever holds the router
   can make every connected machine run its collector. The per-machine allowlist
   bounds what that reaches and the per-machine rate limit bounds how often, but
   the trigger is inherent to query-on-demand.

2. **The daemon cannot verify who is asking.** `asked_by` is the router's claim,
   so a compromised router can attribute a query to someone who never made it.
   `sealmesh audit` renders it as *claimed* for that reason. End-to-end
   signing by the requester is what would close it; it is not built.

3. **The client's GitHub token is broader than the router needs.** `gh auth
   token` hands over whatever the person authorised — typically including
   `repo` — and the router needs only `read:org` plus repo read access. There
   is no way to narrow it from the client; a GitHub App device flow would issue
   a scoped token and is the intended replacement.

## If you are changing this code

[`docs/security-rules.md`](docs/security-rules.md) is the numbered catalog and
the merge checklist. Run the checklist before opening a pull request that
touches routes, auth, the wire format, the collector, a subprocess, a file
read, or a dependency.
