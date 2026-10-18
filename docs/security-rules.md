# Security rules

The rules the code cites as "rule N", and the checklist every change that
touches routes, auth, the wire format, the collector, a subprocess, a file
read or a dependency runs before it merges.

## The one rule that outranks everything else

**No field on the wire may hold text a person wrote.**

Claude Code's `state.json` carries `intent` and `detail`, and those are not
summaries — they are the raw prompt. A live job on the machine this was designed
against held a pasted private Slack thread in `intent`, naming a third party.
`name` and task `subject` are model-authored and carry the same risk by a
different route.

So the rule is not "remember to strip the prompt." Every field on `Snapshot` and
`Agent` in `crates/sealmesh-wire` is an enum, a number, a timestamp, or a newtype whose
constructor enforces a charset and a length. A raw prompt cannot be *assigned* to
any of them. That makes the allowlist a property the compiler checks rather than
a review item someone has to catch. A redaction list would fail open the first
time Claude Code ships a new field; this fails closed.

This mandate covers `Snapshot` and `Agent` and nothing else. Chat, in
`crates/sealmesh-chat-wire`, carries a `Body` — bounded prose, not an
unbounded string — and is deliberately outside this rule rather than a gap in
it: a chat message is prose by definition, so the property that makes this an
allowlist the compiler checks, not a review item, cannot apply to it. Chat's
containment is a different, weaker guarantee, stated in the README's
[Scope](README.md#scope) section rather than this one: a separate opt-in,
default-deny room membership, a hard body cap, and a 7-day retention bound —
not this rule, and not by accident.

If you add a field to the wire:

1. It must not be a `String`. Use an enum, a number, a timestamp, or a newtype.
2. Its `Deserialize` must route **through** the validating constructor
   (`#[serde(try_from = ...)]`), not straight into the inner value. A derive that
   bypasses the constructor defeats the entire design.
3. Add it to `snapshot_field_inventory()` and update the lock test beside it.
4. Bump `SCHEMA_VERSION` if any existing field changed meaning.
5. Say in the PR body what the field can hold and why it cannot hold prose.

Nothing enforces those five but the person opening the PR. There is no gate
left to catch a miss, which is the cost of removing the pipeline and the reason
this rule is stated first.

## Panics are a remote denial of service

`Cargo.toml` sets `unsafe_code = "forbid"` and denies `unwrap_used`,
`expect_used`, `panic`, and `indexing_slicing` for a concrete reason: snapshots
arrive from a process on someone else's laptop. A panic in the reader is an
outage a teammate's machine can trigger, so the fallible paths have to be written
as fallible paths.

Test modules opt out locally with `#![allow(...)]` at the top of each
`mod tests` — never crate-wide.

Clippy does not catch everything. Also avoid: arithmetic that can overflow in
release, byte-index slicing that can land mid-UTF-8, division by a value that can
be zero, and `.nth()` on an iterator you assume is long enough.

## Answers are hostile input

The local collector is lenient; the read path is strict. Unknown fields are
refused, sizes are capped *before* the parser sees the bytes, and every string
goes through a validating newtype.

A failure is contained to one host. If one machine's answer cannot be parsed,
the others still render and that host's row says why. Dropping the host instead
would show a machine running three agents as a machine running nothing, which is
worse than an error.

Errors must not echo attacker-controlled content back out. `WireError::Json`
carries line and column, never the parser's message.

## Attribution comes from the credential

The host a connection speaks for is derived from the **GitHub identity** its
token resolves to — never from a URL path, never from a body, never from config.
There is no `host` field in the config file at all, and a config that tries to
set one is refused rather than having it ignored. Authorisation is **active
membership of one GitHub organisation**, named in the server's own environment
(`SEALMESH_GITHUB_ORG`) and never sent by the client.

Weaker designs were considered and set aside. A storage policy pinning the
*key* a client could write but not the *bytes* would let a compromised client
claim to be a teammate. Hand-distributed bearer tokens would fix that but leave
`host` a string a person chose. A cloud SSO directory would fix that in turn,
but authenticates against a directory that does not contain everyone who should
use this tool — everyone is in the GitHub org.

If you are touching `crates/sealmesh-server/src/auth.rs`, that is the property
you are holding, plus the cost it carries:

- **A token is a bearer credential, so the transport must be encrypted.** A
  signature on a plaintext wire leaks a replayable request; a token on a
  plaintext wire leaks a working GitHub credential for the org. Not the same
  severity. `ServerConfig::connect_url` derives `wss` from `https` so the two
  cannot drift.
- **A token is held for its connection, by that connection's task alone.** A
  reader's stays in memory while its socket is open, because every request it
  makes resolves repo read access with the reader's own credential (`can_read`,
  behind a short per-caller cache). A daemon's does too, because joining a room
  and re-checking that room later resolve access with the daemon's own
  credential, and a post to a ticket room asks GitHub whether that ticket has
  merged with it. Never put in a shared structure, never stored at rest, never
  logged. `SecretToken` has no `Display` and a redacting `Debug`, so this
  survives a careless `tracing` call later.
- **A pending org invitation is not membership.** Someone who has been asked to
  join has not joined.

Do not assume a login has a shape. `host_from_login` normalises rather than
parses, because GitHub controls that format and could widen it.

**The thing to revisit is the token scope:** `gh` hands over whatever the
person authorised, where a GitHub App device flow would issue one scoped to what
the App may do.

The reader validates everything again anyway. `who` checks the router's
attribution against the snapshot's own `host` and refuses the row when they
disagree — the router is a service that can itself be compromised, so it does not
get to relabel someone's work.

## The router is a remote read trigger

Query-on-demand is better than publish-on-interval for privacy, and worse in one
specific way that must not be forgotten: whoever holds the router can make every
connected laptop run its collector. The allowlist gates on each machine bound
what that can reach, and each daemon records who asked in `sealmesh audit` —
but the daemon cannot verify the router's claim about *who* is asking, so that
record is only as truthful as the router. End-to-end signing by the requester
would close it and is not yet done.

If you add a reason for the router to reach into a machine, that is the ledger
you are adding to.

## Security rules

The sections above are the *reasoning* — why this system is shaped the way it
is. This is the *checklist*, and every rule is a blocking issue rather than a
preference.

It started from a general 37-rule catalog and was rewritten for Rust and for
this system. **Numbering is kept, holes and all**, because the code cites rules
by number. Rules that cannot apply here are listed as dropped, with the reason
and the condition that would bring them back. Rules 38–45 are new: Rust and this
codebase have failure modes a general catalog has no number for.

There used to be a runnable counterpart that grepped for violations of these.
It is gone. Read this list against the diff yourself before opening a PR that
touches routes, auth, the wire format, the collector, a subprocess, a file read,
or a dependency — the checklist at the end of the section is the short form.

### Authentication & authorization (1–5)

1. **Every network-reachable route requires authentication by default.** The
   only public route is `GET /v1/healthz`, which takes no input and returns no
   data. A new public route needs a comment saying why, and the reason has to be
   "liveness" — there is no auth-bootstrap case here, because the credential
   arrives in the first frame rather than being issued by us.
2. **Auth goes in at the route definition, not "later."** On this server that
   means inside `run` before the role branch, so neither `serve_daemon` nor
   `serve_reader` can be reached unauthenticated. A route handler merged with a
   `// TODO: add auth` is a blocking issue.
3. *Dropped — no prefix to guard.* There are two routes and the handshake runs
   once, before either. Restore this if the router ever grows a REST surface.
4. **Authentication is not authorization. Check the caller may reach the
   specific thing, not just that they are someone.** Being in the org is not
   permission to query a particular machine. Every answer is narrowed to agents
   in repos the reader's own token can read, and a narrowed answer says so with
   `ServerFrame::Partial` — see `query.rs`. What remains is the question rather
   than the answer: `ask_all` still asks every registered host for any
   authenticated reader, because the router cannot know a machine's repos
   before asking it, so each daemon collects (and records who asked) for a
   query that may see none of its work.
5. **Re-audit auth coverage when you touch routing.** Do not trust "it was
   protected before" — grep every `.route(` and confirm each one is either
   behind the handshake or on the documented public list.

### Secrets & credentials (6–8)

6. **Never commit a secret.** Tokens, keys, `.env`, a certificate ARN is fine,
   a private key is not. If one appears in a diff, stop and say so before doing
   anything else. If one was already committed, it is leaked — rotate first,
   then rewrite history.
7. **Never log a credential, and make that structural rather than careful.**
   `SecretToken` has no `Display` and a redacting `Debug` precisely so a
   `tracing` call added in two years cannot leak it. A new credential type gets
   the same treatment and a test that asserts it, because "we remembered" is not
   a property.
8. **No hardcoded fallback for an input that decides authorization.**
   `SEALMESH_GITHUB_ORG` is required with no default, and that is the rule
   rather than an accident: a default is a guess about who may connect, and
   guessing wrong authorises the wrong people instead of refusing to start. An
   `unwrap_or_else(|_| "…")` on an authorization input is a blocking issue.
   `SEALMESH_PROJECTS` is the same kind of input — it decides which project
   room a repo's agents may join — so it too is required, and a malformed value
   refuses to start rather than loading part of a map. `[]` is a stated empty
   mapping, not a default.
   `SEALMESH_LISTEN` may default, because it decides where to bind and not who
   gets in — and `listen::permit_plaintext` guards what that default exposes.
   `SEALMESH_ROOM_TOKENS_PER_HOUR` may also default (20,000), because it
   bounds what one host may post to one room, not who gets in — room membership
   is still decided by the join check. `room_budget::TokensPerHour` accepts only
   5,350 (one maximum-size post) to 200,000, a ceiling enforced in code by its
   only constructor, and a value that is not valid UTF-8, not a whole number, or
   outside those bounds refuses to start rather than being clamped.
   `SEALMESH_ROOM_TURN_CAP` may also default (20 posts), for the same reason:
   it bounds how many turns two hosts may take in one room with no third
   host posting, not who gets in. `turn_cap::TurnCap` accepts only 4 to 100, a
   ceiling enforced in code by its only constructor, and any other value
   refuses to start rather than being clamped.
   `SEALMESH_ACCESS_CACHE_TTL_SECS` may also default (30s), because it
   bounds how stale an already-resolved access answer may be, not who is
   authorised — the authorisation decision itself still runs on every cache
   miss, unaffected by this variable. `access_cache::MAX_TTL` (300s) is a hard
   ceiling enforced in code regardless of what this reads, and a value that is
   malformed or exceeds that ceiling refuses to start rather than silently
   clamping. `0` is a legal explicit value — it disables caching outright
   rather than being read as "unset."
   `SEALMESH_READER_CHECKS_PER_HOUR` may also default (1,000), because it
   bounds how much of a reader's GitHub rate limit other machines' answers can
   spend, not who may read — a check it refuses withholds that row and marks
   the answer partial, never grants it. `check_budget::ChecksPerHour` accepts
   only 64 (one burst an hour) to 2,000, a ceiling enforced in code by its only
   constructor, and a value that is not valid UTF-8, not a whole number, or
   outside those bounds refuses to start rather than being clamped. `0` is
   below the floor, not a way to turn the budget off. A cache hit is free, so
   with `SEALMESH_ACCESS_CACHE_TTL_SECS=0` every check is charged, and a
   reader asking often about many repos reads partial much sooner.
   The allowance is per credential-derived host, not per reader connection:
   a daemon's room listing (`ClientFrame::ListRooms`) charges each uncached
   join check to the same host's allowance, at most 32 per listing, because
   both spend the same person's GitHub rate limit with the same credential,
   and a separate allowance would double what that person's machines can take
   of it. A check the allowance refuses leaves that room out of the listing,
   never lists it. The cost is that a daemon listing often makes its own
   host's `who` read partial sooner.

### Input validation & injection (9–12)

9. **Validate at the boundary with a type, not with an `if`.** Every string
   crossing the wire goes through a newtype whose `TryFrom` enforces a charset
   and a length. An ad-hoc `if s.len() < N && s.chars().all(…)` at a call site is
   a check somebody will forget at the second call site.
10. *Dropped — no query language.* There is no database, no ORM, and nothing
    that parses a query string. Restore this the moment a store lands; the
    transaction-log work is the likely trigger.
11. **Do not build a subprocess invocation out of values you did not choose.**
    In Rust the shell is not the risk — `Command` with explicit args already
    defeats metacharacters. The risk is the callee's own flag parsing: `git`
    reads a ref named `--upload-pack=…` as a flag, so a *branch name* is an
    argument-injection vector with no shell anywhere. Every `Command::new` here
    has a fixed argv and takes nothing from a snapshot. Prefer no subprocess at
    all: `collect/git.rs` uses `gix` in-process for exactly this reason.
12. **Never trust a client-supplied identity.** This is the rule the whole
    system is built around. `HostId` comes from the credential, never from a
    path, a body, or a config file — and there is no `host` field in the config
    at all, so a config that tries to set one is *refused* rather than ignored.
    A frame field that names who the sender is, is a blocking issue.

### Sensitive data (13–16)

13. **Keep private data out of URLs and log lines.** No host in a URL path, no
    working directory in anything that leaves the machine. `doctor` prints
    `work_dir` on the owner's own terminal, which is the point of a diagnostic
    and the only place it is allowed.
14. **Do not echo attacker-controlled content back out through an error.**
    `WireError::Json` and `ProtoError::Json` carry a line and a column and never
    the parser's message, because serde quotes the value it choked on. An error
    type that interpolates bytes from a peer is a blocking issue.
15. **Carry the minimum on the wire, and adding to it is a mandate, not a
    field.** See "the one rule that outranks everything else". A new `Snapshot`
    or `Agent` field is five checks and a PR-body paragraph.
16. **A read of someone's activity is recorded where they can see it.**
    `sealmesh audit` is that record. Adding a reason for the router to reach
    into a machine means adding to that ledger in the same change.

### Frames & peer trust (20–21)

17–19, 22. *Dropped — no browser.* No cookies, no server-side session, no CSRF
surface. Restore if a web UI is ever added; a session cookie carrying this
system's identity would need all four back at once.

20. **A frame must be attributable before it causes work.** The webhook-signature
    rule in its natural shape here. A `Query` is only acted on after `Welcome`,
    and a daemon's `Answer` resolves only a ticket the router actually issued —
    a forged `QueryId` is dropped rather than allowed to resolve someone else's
    query. If a secret is ever compared, compare it in constant time; `==` on a
    credential is a blocking issue.
21. **"It came from our own service" is not authentication.** The daemon does
    not trust the router's claim about who is asking, and `who` re-checks the
    router's attribution against the snapshot's own `host` and refuses the row
    when they disagree. The router is a service that can itself be compromised;
    it does not get to relabel someone's work.

### Rate limiting & abuse (23–25)

23. **The step that authenticates is the step that gets attacked.** It carries a
    deadline (`HANDSHAKE_TIMEOUT`) so an unauthenticated stranger cannot hold a
    connection task. A concurrent-connection cap is the open item.
24. **Anything that triggers cost on someone else's machine needs a cap, and the
    cap belongs on the machine paying.** A query runs the collector across every
    allowed checkout, so `QueryBudget` lives in the daemon — a limit enforced by
    the party it constrains is not a limit.
25. **A refusal must not say which check failed.** `Denied` is deliberately
    coarse: "that token is not valid" and "you are not in the org" are the same
    answer to anyone probing, and the difference only helps someone who already
    holds the token and can read their own `gh auth status`.

### Errors & logging (26–28)

26. **No internal detail crosses the wire on an error.** In Rust that is mostly
    handled by rule 39 — a panic is a `500` with a backtrace in the log and a
    dead task — but an `anyhow` chain rendered into a frame is the same leak by
    a different route.
27. **Never log a frame that contains a credential.** `ClientFrame::Hello`
    carries a token; its `Debug` is safe because `SecretToken`'s is, and that is
    the only reason.
28. **Log authentication failures and rate-limit refusals.** They are the signal
    that something is happening. A refused query writes an audit entry precisely
    so a flood is visible to the person being flooded.

### Dependencies & supply chain (29–31)

29. **Pin what gets installed, everywhere it gets installed.** `Cargo.lock` is
    committed and the Dockerfile builds `--locked`. This extends past cargo:
    every GitHub Action is pinned to a commit SHA (CI refuses a mutable tag),
    and every downloaded tool is pinned by version and checksum. A mutable ref
    is third-party code choosing what runs in our jobs.
30. **Do not add a dependency for one function.** Declared-and-unused is not
    free either: a crate in `[workspace.dependencies]` that nothing uses reads
    as approved surface.
31. **`cargo deny check advisories` is signal.** An ignore is a decision that
    needs a line of prose in `deny.toml` next to it, not a silent `--allow`.

### Filesystem (33)

32, 34. *Dropped — no uploads.* Nothing accepts a file from anyone.

33. **A path component that came from outside is validated before it is
    joined.** `PathBuf::join` does not sanitise: an absolute component discards
    everything before it and `..` walks up. `is_path_segment` gates `sessionId`
    at the point it becomes a `Candidate`, so an unsafe one cannot be *held*,
    and `Gate::allowed_dir` canonicalises **before** matching a prefix so a
    symlink or a `..` cannot walk out of an allowed root.

### Transport & surface (36–37)

35. *Dropped — no HTML.* Nothing renders a page.

36. **The credential is in the frame, not in ambient state, and it must stay
    there.** This is what makes the WebSocket endpoint safe without an `Origin`
    check: a page in someone's browser can open a socket to `/v1/connect`, and
    it gets `Denied`, because there is no cookie for it to ride. Moving the
    credential into anything the browser attaches automatically would need an
    origin allowlist in the same change.
37. **Refuse what you do not handle.** `get()` on both routes rejects other
    methods, `deny_unknown_fields` on every frame refuses a field we do not
    know, and unknown roles do not exist because `Role` is a closed enum.

### Rust and this codebase (38–45)

38. **`unsafe_code = "forbid"`.** Workspace-wide, no exceptions, no local
    `allow`.
39. **A panic is a remote denial of service.** `unwrap_used`, `expect_used`,
    `panic`, and `indexing_slicing` are denied outside tests because input
    arrives from a process on someone else's laptop. Test modules opt out with
    `#![allow(...)]` at the top of each `mod tests` — never crate-wide.
40. **The arithmetic clippy does not catch is still yours.** Overflow in
    release, byte-index slicing that can land mid-UTF-8, division by a value
    that can be zero, `.nth()` on an iterator you assume is long enough.
    `QueryBudget::earn` guards a zero interval for exactly this reason, even
    though the constant it is built from cannot be zero.
41. **The wire-format mandate.** No `String` on `Snapshot` or `Agent`, ever.
    Five checks, in "the one rule that outranks everything else", and nothing
    but the author checks them.
42. **Cap sizes before the parser, not after.** A limit applied once the parser
    has finished is not a limit. `MAX_SNAPSHOT_BYTES`, `MAX_FRAME_BYTES` (at the
    transport, so an oversized frame never reaches `decode`), and
    `MAX_RESPONSE_BYTES` on the GitHub read path.
43. **`deny_unknown_fields` on a peer protocol; lenient on a producer you do not
    control.** Frames and snapshots are strict because both ends ship together
    and drift means something is wrong. `RawJob` is lenient on purpose — Claude
    Code adds fields without telling us, discarding is the *safer* direction
    because `intent` and `detail` are dropped at the parse boundary, and being
    strict would turn a Claude Code release into an outage. Getting this
    backwards in either direction is a blocking issue. One stated exception:
    the daemon reads a refusal whose category it does not know as `unknown`
    rather than dropping it (see Design decisions), because a lost refusal
    fails silently and laptops upgrade by hand.
44. **Every read a remote request can trigger is bounded — bytes, count, and
    file type.** The router is a remote read trigger, so an unbounded read
    inside the allowlist gates is a hole in the argument that the gates bound
    the blast radius. Type matters as much as size: opening a fifo blocks
    forever, and a daemon that is not answering is a machine that has vanished.
45. **A `Deserialize` on a read path routes through the validating
    constructor.** `#[serde(try_from = "…")]`, not a derive over public fields.
    A derive that bypasses the constructor defeats the design while looking
    exactly like it.

## Before merging — security checklist

For any change touching routes, auth, the wire format, the collector, a
subprocess, a file read, or a dependency:

- [ ] Every new or moved route is behind the handshake, or is on the documented
      public list with a justification (rules 1, 2, 5).
- [ ] Nothing decides identity or authorization from a value the client sent
      (rule 12).
- [ ] No secrets in the diff, and no credential that can reach a log line
      through `Display` or `Debug` (rules 6, 7, 27).
- [ ] No env read with a hardcoded fallback for an authorization input (rule 8).
- [ ] Every value crossing a boundary goes through a validating newtype, and its
      `Deserialize` routes through the constructor (rules 9, 45).
- [ ] Errors carry a position or a category, never bytes that came from a peer
      (rules 14, 26).
- [ ] Every new read or subprocess a remote request can trigger is bounded in
      size, count, and file type, and takes no argument from a snapshot
      (rules 11, 42, 44).
- [ ] If the wire format changed: all five points of the mandate, plus the
      canary test (rule 41).
- [ ] If a dependency or an Action was added or bumped: `cargo deny check`
      is clean, it is pinned, and it is worth more than the surface (rules
      29, 30, 31).
