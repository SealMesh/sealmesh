//! `sealmesh chat`: list live rooms, or tail one live — read-only.
//!
//! There is no post path here. A human reads a room through this command and
//! writes into it nowhere: writing is a second consent surface and a
//! separate decision, out of scope for this command by design.
//!
//! This talks to the *daemon's local socket* (`room::connect`), never the
//! router directly — the daemon already holds the one connection per host
//! that room frames travel over, and joining, backfilling and
//! tailing all reuse the same request types the hook and the MCP room tools
//! use (`room::local`).
//!
//! Every daemon or transport failure renders as one clear line and this never
//! hangs: `Tail`'s wait is capped by the daemon at ten seconds
//! (`local::MAX_TAIL_WAIT_MS`), every request carries
//! `connect::DEFAULT_DEADLINE`, and a decode failure on one message in a
//! `Tail` batch is rendered in place — the batch's other messages, and the
//! cursor, still advance — while a whole answer that will not decode at all
//! is retried a bounded number of times with a short backoff between
//! attempts, rather than either wedging forever or taking a whole session
//! down over one bad frame (rule 39) — see `MAX_CONSECUTIVE_DECODE_ERRORS`.
//!
//! A scope this machine's credential cannot join reads exactly like one that
//! does not exist ([`NO_ROOM`], rule 25): `Resolved` naming no matching room
//! and `JoinRefused` are the same line, and neither is treated as a command
//! failure — both print [`NO_ROOM`] and exit cleanly.
//!
//! Every `chat` joins and leaves under its own claim, `chat:<pid>` (see
//! `room::local::Request::Join`'s `holder`), so the daemon — not this
//! process — decides whether a room actually needs leaving: a claim already
//! held by the hook, an MCP tool, or another `chat` keeps the room alive
//! under the daemon's own bookkeeping, and this process never needs to check
//! `Status` before joining. Leaving is unconditional and best-effort on
//! every exit path once a join has at least been attempted — normal
//! completion, an error, Ctrl-C, SIGTERM or SIGHUP — because a `Join` may
//! already have reached the daemon even if this process is cancelled before
//! seeing the answer; a `Leave` for a claim that never landed is a harmless
//! no-op the daemon answers `not_member`. Ctrl-C, SIGTERM and SIGHUP are all
//! watched for from before the first request (resolving the scope) through
//! the last (a `Backfill` page or a `Tail` poll), by one signal listener
//! installed once and re-awaited at every point that can block, so a signal
//! landing anywhere in that window is caught — not only one landing between
//! `Tail` polls.
//!
//! Every string this prints that came from a room — a body, a host, a room
//! name — is sanitised before it reaches a terminal (`render::sanitize`):
//! ANSI/OSC escapes, NUL and bidi overrides are neutralised, and a
//! multi-line body keeps its line shape. Nothing here emits color or other
//! styling at all — the safest way to respect `NO_COLOR` is to never need to
//! check it.

use std::{future::Future, io::Write, path::Path, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use jiff::Timestamp;
use sealmesh_chat_wire::{Body, MAX_BODY_CHARS, MessageKind, RoomId, Scope};
use sealmesh_proto::RoomSeq;
use sealmesh_wire::{HostId, RepoSlug, TicketId};
use tokio::sync::watch;

use crate::{
    gitref, render,
    room::{
        connect::{self, DEFAULT_DEADLINE},
        local::{ConsumerKey, Delivered, MAX_TAIL_WAIT_MS, Request, Response, TailCursor},
        room_name, socket, word,
    },
};

/// What `<scope>` named on the command line.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ChatScope {
    Ticket(TicketId),
    Repo(RepoSlug),
    Project,
}

/// Printed for a scope that resolves to no room and for a join the router
/// refuses — deliberately the same line for both (rule 25): telling them
/// apart would tell a caller whether a room exists that their own credential
/// cannot read.
pub const NO_ROOM: &str =
    "no room for that scope: it may not exist, or you may not have access to it";

/// How many whole `Tail` answers in a row may fail to decode before this
/// gives up and reports it, rather than retrying forever. A single bad
/// *message* inside an otherwise-readable answer never counts against this —
/// it renders in place and the batch's cursor still advances (rule 39) — this
/// is only for an answer so broken it cannot even be read as a `Tail` shape
/// at all. A daemon whose answers never become readable again is a version
/// mismatch, not noise, and retrying that without bound would be the hang
/// rule 39's neighbouring rules forbid.
const MAX_CONSECUTIVE_DECODE_ERRORS: u32 = 3;

/// How long this waits between two decode-failure retries, so a persistently
/// broken daemon is not hammered three times in a row with no pause.
const DECODE_RETRY_BACKOFF: Duration = Duration::from_millis(200);

/// Long enough that a legal `HostId` (32 characters at most) is never
/// truncated. A defensive bound on this build's own assumption about that
/// cap, not a limit a caller is meant to notice.
const MAX_HOST_RENDER_CHARS: usize = 64;

/// Parses `<scope>`: the literal `project`, an `owner/repo`, or a ticket like
/// `XYZ-62`. Case-insensitive on the ticket prefix and the `project`
/// keyword, since a person types this, not a peer.
fn parse_scope(raw: &str) -> Result<ChatScope> {
    if raw.eq_ignore_ascii_case("project") {
        return Ok(ChatScope::Project);
    }
    if let Ok(repo) = RepoSlug::try_from(raw) {
        return Ok(ChatScope::Repo(repo));
    }
    if let Some(ticket) = parse_ticket(raw) {
        return Ok(ChatScope::Ticket(ticket));
    }
    bail!("'{raw}' is not a scope: use `project`, an `owner/repo`, or a ticket like `XYZ-62`")
}

fn parse_ticket(raw: &str) -> Option<TicketId> {
    let (prefix, number) = raw.split_once('-')?;
    let number: u32 = number.parse().ok()?;
    TicketId::new(&prefix.to_ascii_uppercase(), number).ok()
}

/// This invocation's own claim, distinct from any other `chat`, the hook, or
/// an MCP tool: `chat:<pid>`. Two `chat` processes on the same machine (a
/// tail left running, plus a second one) hold two different claims, and each
/// must leave before the room actually does.
fn chat_holder() -> Result<ConsumerKey> {
    let label = format!("chat:{}", std::process::id());
    ConsumerKey::new(&label)
        .map_err(|e| anyhow!("could not build this invocation's own claim label: {e}"))
}

/// The CLI entry point: `sealmesh chat [<scope>] [--since <seq>]`.
pub async fn run(scope: Option<String>, since: Option<u64>) -> Result<()> {
    let path = socket::default_path()
        .ok_or_else(|| anyhow!("no home directory to find the daemon's socket under"))?;
    let checkout = std::env::current_dir().context("reading the current directory")?;
    let mut stdout = std::io::stdout();
    run_at(&mut stdout, &path, &checkout, scope, since).await
}

/// `run`, with the daemon's socket and the checkout a bare scope is read
/// from passed in rather than taken from the process.
async fn run_at<W: Write>(
    out: &mut W,
    path: &Path,
    checkout: &Path,
    scope: Option<String>,
    since: Option<u64>,
) -> Result<()> {
    match scope {
        None => list(out, path).await,
        Some(raw) => {
            let scope = parse_scope(&raw)?;
            tail(out, path, checkout, &scope, since.map(RoomSeq)).await
        }
    }
}

async fn ask(path: &Path, request: &Request) -> Result<Response> {
    Ok(connect::request(path, request, DEFAULT_DEADLINE).await?)
}

/// The line for a `Response` this call site did not expect, but that names a
/// daemon- or router-level condition every request can answer with. `None`
/// means `response` was not one of those — the caller's own business.
fn daemon_failure_line(response: &Response) -> Option<&'static str> {
    match response {
        Response::Disabled {} => Some(
            "chat is disabled on this machine ([chat] enabled = false, or its kill file exists)",
        ),
        Response::NotConnected {} => Some("this daemon has no connection to the router right now"),
        Response::Busy {} => Some("the daemon is busy right now; try again shortly"),
        Response::TimedOut {} => Some("timed out waiting for the router to answer"),
        // `holder` is additive (`#[serde(default)]`), so this build's own
        // requests still decode on an old daemon — but a daemon old enough
        // to predate `holder` predates every other request this module can
        // send too, so `invalid` in practice means exactly this.
        Response::Invalid {} => Some(
            "the daemon rejected that request — it may be running an older build that does not \
             understand it yet. Restart it",
        ),
        Response::TooLarge {} => Some("the daemon's answer would not fit; try again"),
        _ => None,
    }
}

/// The error for a `Response` this call site did not expect: `response`'s own
/// failure line if it has one, else a generic line naming no bytes (rule 39 —
/// never panic on a response shape this build was not written for).
fn unexpected(op: &str, response: &Response) -> anyhow::Error {
    match daemon_failure_line(response) {
        Some(line) => anyhow!(line),
        None => anyhow!("the daemon answered `{op}` with something this build does not expect"),
    }
}

async fn list<W: Write>(out: &mut W, path: &Path) -> Result<()> {
    let response = ask(path, &Request::ListRooms {}).await?;
    match response {
        Response::Rooms { rooms, age_secs } => {
            let rooms = rooms.as_slice();
            if rooms.is_empty() {
                let _ = writeln!(out, "no rooms are live right now");
                return Ok(());
            }
            if age_secs > 0 {
                let _ = writeln!(
                    out,
                    "(reusing a listing from {age_secs}s ago rather than asking again yet)"
                );
            }
            for room in rooms {
                let _ = writeln!(out, "{}", room_name(room));
            }
            Ok(())
        }
        Response::ListThrottled { retry_after_secs } => {
            let _ = writeln!(
                out,
                "a listing was asked for too recently; try again in {retry_after_secs}s"
            );
            Ok(())
        }
        Response::RoomListRefused {} => {
            let _ = writeln!(
                out,
                "a room listing is already running on this daemon; try again shortly"
            );
            Ok(())
        }
        other => Err(unexpected("list_rooms", &other)),
    }
}

/// The checkout's GitHub repo, for a scope that needs one (`project`, or a
/// bare ticket) rather than naming its own repo directly.
fn current_repo(checkout: &Path) -> Result<RepoSlug> {
    gitref::read(checkout, &[])
        .map(|checkout| checkout.repo)
        .ok_or_else(|| {
            anyhow!(
                "that scope needs the current checkout's GitHub repo, and none was found here \
                 (not a git checkout, or its origin is not on GitHub)"
            )
        })
}

/// The room `scope` names, from the daemon's own resolution — never built
/// locally, since only the router's project map says which project a repo is
/// in. `None` when nothing in the resolved list matches (rule 25: the same
/// as a room that does not exist).
async fn resolve_target(path: &Path, checkout: &Path, scope: &ChatScope) -> Result<Option<RoomId>> {
    let (repo, ticket) = match scope {
        ChatScope::Repo(repo) => (repo.clone(), None),
        ChatScope::Ticket(ticket) => (current_repo(checkout)?, Some(ticket.clone())),
        ChatScope::Project => (current_repo(checkout)?, None),
    };
    let response = ask(
        path,
        &Request::Resolve {
            repo,
            ticket,
            join: false,
        },
    )
    .await?;
    let Response::Resolved { rooms } = response else {
        return Err(unexpected("resolve", &response));
    };
    Ok(rooms.into_iter().find_map(|resolved| {
        let matches = match (scope, &resolved.room.scope) {
            (ChatScope::Repo(_), Scope::Repo(_)) => true,
            (ChatScope::Ticket(wanted), Scope::Ticket(got)) => wanted == got,
            (ChatScope::Project, Scope::Project) => true,
            _ => false,
        };
        matches.then_some(resolved.room)
    }))
}

/// Watches for Ctrl-C, SIGTERM or SIGHUP — whichever arrives first — and
/// resolves once, installed a single time per invocation and re-awaited from
/// every point that can block (resolving the scope, joining, backfilling,
/// tailing), so a signal landing anywhere in that window is caught, not only
/// one landing between `Tail` polls.
struct Shutdown(watch::Receiver<bool>);

impl Shutdown {
    fn install() -> Self {
        let (tx, rx) = watch::channel(false);
        tokio::spawn(async move {
            wait_for_signal().await;
            let _ = tx.send(true);
        });
        Self(rx)
    }

    /// Drives this straight from a `watch::Receiver` instead of a real
    /// signal, so a test can flip it at an exact, controlled point — for
    /// example strictly before a fake daemon sends the answer that ends one
    /// `Tail` poll, proving a signal landing between two polls (not only
    /// during one) is still caught by the one `Shutdown` a whole `tail`
    /// call shares, rather than a fresh one somehow recreated per poll.
    #[cfg(test)]
    fn from_watch(rx: watch::Receiver<bool>) -> Self {
        Self(rx)
    }

    /// Resolves once a signal has arrived — at once if one already has by the
    /// time this is called, however many times this is called.
    async fn triggered(&mut self) {
        if *self.0.borrow() {
            return;
        }
        let _ = self.0.changed().await;
    }
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    // A signal this platform does not have, or one this process could not
    // install a handler for, is treated as "never arrives" rather than an
    // error: the other two still watch for it, and installing none of them
    // would be the one failure mode that could stop a claim from ever being
    // released.
    let mut term = signal(SignalKind::terminate()).ok();
    let mut hup = signal(SignalKind::hangup()).ok();
    let recv_ctrl_c = async {
        // `tokio::signal::ctrl_c()` fails if a handler cannot be installed,
        // and an `Err` there means exactly the same as no handler for
        // SIGTERM/SIGHUP above: never treat "could not install" as "already
        // arrived", or a sandboxed or double-registered process would shut
        // down before doing any work at all.
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    let recv_term = async {
        match &mut term {
            Some(s) => {
                s.recv().await;
            }
            None => std::future::pending::<()>().await,
        }
    };
    let recv_hup = async {
        match &mut hup {
            Some(s) => {
                s.recv().await;
            }
            None => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        () = recv_ctrl_c => {}
        () = recv_term => {}
        () = recv_hup => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    if tokio::signal::ctrl_c().await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Races `fut` against the shutdown signal. `None` means the signal won —
/// the caller stops and lets whatever cleanup it already owes (a `Leave`,
/// bounded by its own deadline) run regardless.
async fn race<T>(signals: &mut Shutdown, fut: impl Future<Output = T>) -> Option<T> {
    tokio::select! {
        biased;
        () = signals.triggered() => None,
        value = fut => Some(value),
    }
}

async fn tail<W: Write>(
    out: &mut W,
    path: &Path,
    checkout: &Path,
    scope: &ChatScope,
    since: Option<RoomSeq>,
) -> Result<()> {
    tail_with(out, path, checkout, scope, since, Shutdown::install()).await
}

/// `tail`, taking its `Shutdown` rather than installing a real one, so a
/// test can drive it from a plain `watch::Receiver` flipped at an exact,
/// controlled point.
async fn tail_with<W: Write>(
    out: &mut W,
    path: &Path,
    checkout: &Path,
    scope: &ChatScope,
    since: Option<RoomSeq>,
    mut signals: Shutdown,
) -> Result<()> {
    let holder = chat_holder()?;

    let target = match race(&mut signals, resolve_target(path, checkout, scope)).await {
        None => return Ok(()),
        Some(result) => match result? {
            Some(target) => target,
            None => {
                let _ = writeln!(out, "{NO_ROOM}");
                return Ok(());
            }
        },
    };

    // From here on a `Join` may reach the daemon even if this process is
    // cancelled before seeing the answer, so from here on this always
    // attempts a matching `Leave` on the way out, whatever happens next.
    let joined = race(
        &mut signals,
        ask(
            path,
            &Request::Join {
                room: target.clone(),
                holder: Some(holder.clone()),
            },
        ),
    )
    .await;

    let result: Result<()> = 'outcome: {
        let response = match joined {
            None => break 'outcome Ok(()),
            Some(Err(e)) => break 'outcome Err(e),
            Some(Ok(response)) => response,
        };
        match response {
            Response::Joined { .. } => {}
            Response::JoinRefused { .. } => {
                let _ = writeln!(out, "{NO_ROOM}");
                break 'outcome Ok(());
            }
            Response::RoomLimit {} => {
                break 'outcome Err(anyhow!(
                    "this daemon already holds as many rooms as it can; leave one first"
                ));
            }
            other => break 'outcome Err(unexpected("join", &other)),
        }
        run_tail(out, path, &target, since, &mut signals).await
    };

    // Best-effort: a failed leave here must not shadow the real result, and
    // there is nothing more useful to do with one than to have tried it
    // once. A leave for a claim that never landed is a harmless no-op the
    // daemon answers `not_member`.
    let _ = connect::request(
        path,
        &Request::Leave {
            room: target.clone(),
            holder: Some(holder),
        },
        DEFAULT_DEADLINE,
    )
    .await;
    result
}

async fn run_tail<W: Write>(
    out: &mut W,
    path: &Path,
    target: &RoomId,
    since: Option<RoomSeq>,
    signals: &mut Shutdown,
) -> Result<()> {
    if let Some(after) = since {
        match race(signals, backfill_all(out, path, target, after)).await {
            None => return Ok(()),
            Some(result) => result?,
        }
    }
    let mut cursor: Option<TailCursor> = None;
    let mut consecutive_decode_errors: u32 = 0;
    loop {
        let request = Request::Tail {
            room: target.clone(),
            after: cursor,
            wait_ms: Some(MAX_TAIL_WAIT_MS),
        };
        let Some(outcome) = race(
            signals,
            connect::request_bytes(path, &request, DEFAULT_DEADLINE),
        )
        .await
        else {
            return Ok(());
        };
        let bytes = outcome?;
        let answer = match decode_tail_answer(&bytes) {
            Ok(answer) => {
                consecutive_decode_errors = 0;
                answer
            }
            Err(e) => {
                consecutive_decode_errors += 1;
                let _ = writeln!(
                    out,
                    "(the daemon's answer could not be read — line {}, column {})",
                    e.line(),
                    e.column()
                );
                if consecutive_decode_errors >= MAX_CONSECUTIVE_DECODE_ERRORS {
                    bail!(
                        "the daemon's answers keep failing to decode; it may be running an \
                         older build. Restart it"
                    );
                }
                if race(signals, tokio::time::sleep(DECODE_RETRY_BACKOFF))
                    .await
                    .is_none()
                {
                    return Ok(());
                }
                continue;
            }
        };
        match answer {
            TailAnswer::Tail {
                messages,
                cursor: next,
                lost,
                reset,
            } => {
                if reset {
                    let _ = writeln!(
                        out,
                        "(this room restarted in the daemon; some earlier messages may be missing)"
                    );
                }
                if lost > 0 {
                    let _ = writeln!(
                        out,
                        "({lost} messages were dropped here before they could be read)"
                    );
                }
                for message in messages {
                    match message {
                        TailMessage::Ok(delivered) => {
                            let _ = writeln!(
                                out,
                                "{}",
                                render_message(
                                    delivered.seq,
                                    &delivered.from,
                                    &delivered.kind,
                                    &delivered.body,
                                    Some(delivered.received_at)
                                )
                            );
                        }
                        TailMessage::Malformed => {
                            let _ = writeln!(out, "(one message in this batch could not be read)");
                        }
                    }
                }
                cursor = Some(next);
            }
            TailAnswer::Other(Response::NotMember { .. }) => {
                bail!("this daemon no longer holds that room")
            }
            TailAnswer::Other(other) => return Err(unexpected("tail", &other)),
        }
    }
}

/// A `Tail` answer, decoded leniently: everything except `messages` matches
/// `Response::Tail` exactly.
enum TailAnswer {
    Tail {
        messages: Vec<TailMessage>,
        cursor: TailCursor,
        lost: u64,
        reset: bool,
    },
    /// Any other response shape, decoded exactly as strictly as
    /// `local::decode` always has.
    Other(Response),
}

/// One entry from a `Tail` answer's `messages` array, decoded on its own.
enum TailMessage {
    Ok(Delivered),
    Malformed,
}

/// The lenient mirror of `Response::Tail` used to decode one first: every
/// field matches `Response::Tail` exactly except `messages`, kept as raw
/// JSON until each entry is decoded on its own.
#[derive(serde::Deserialize)]
struct LenientTail {
    messages: Vec<serde_json::Value>,
    cursor: TailCursor,
    lost: u64,
    reset: bool,
}

/// Decodes one `Tail` poll's raw answer leniently: a `result` of anything but
/// `"tail"` decodes exactly as every other response in this module does
/// (rule 45), and a `"tail"` decodes each of its `messages` on its own, so
/// one that does not decode renders as an error in place instead of taking
/// the rest of the batch — and the room — down with it (rule 39). Bytes are
/// already capped by `connect::request_bytes` before this ever sees them
/// (rule 42).
fn decode_tail_answer(bytes: &[u8]) -> Result<TailAnswer, serde_json::Error> {
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    if value.get("result").and_then(serde_json::Value::as_str) != Some("tail") {
        return serde_json::from_value::<Response>(value).map(TailAnswer::Other);
    }
    let lenient: LenientTail = serde_json::from_value(value)?;
    let messages = lenient
        .messages
        .into_iter()
        .map(|raw| match serde_json::from_value::<Delivered>(raw) {
            Ok(delivered) => TailMessage::Ok(delivered),
            Err(_) => TailMessage::Malformed,
        })
        .collect();
    Ok(TailAnswer::Tail {
        messages,
        cursor: lenient.cursor,
        lost: lenient.lost,
        reset: lenient.reset,
    })
}

/// Reads `target`'s stored history after `since`, paginated through
/// `Backfill`'s `after`/`next`, printing each page as it arrives. A refusal
/// or a page that makes no forward progress stops backfilling and says why,
/// but does not fail the command: the live tail that follows is still worth
/// having.
async fn backfill_all<W: Write>(
    out: &mut W,
    path: &Path,
    target: &RoomId,
    since: RoomSeq,
) -> Result<()> {
    let mut after = Some(since);
    loop {
        let response = ask(
            path,
            &Request::Backfill {
                room: target.clone(),
                after,
            },
        )
        .await?;
        match response {
            Response::Backfill { messages, next, .. } => {
                for message in &messages {
                    let _ = writeln!(
                        out,
                        "{}",
                        render_message(
                            message.seq,
                            &message.from,
                            &message.kind,
                            &message.body,
                            None
                        )
                    );
                }
                // Strictly forward only: a cursor that repeats or goes
                // backwards would loop here forever rather than progress.
                let progressed = match (next, after) {
                    (Some(n), Some(a)) => n > a,
                    (Some(_), None) => true,
                    (None, _) => false,
                };
                match next {
                    Some(n) if progressed => after = Some(n),
                    Some(_) => {
                        let _ = writeln!(out, "(history read stopped without making progress)");
                        return Ok(());
                    }
                    None => return Ok(()),
                }
            }
            Response::BackfillRefused { category, .. } => {
                let _ = writeln!(out, "(history could not be read: {})", word(&category));
                return Ok(());
            }
            other => return Err(unexpected("backfill", &other)),
        }
    }
}

/// Renders one message: a sanitised header naming its sequence, host and
/// kind (plus when it arrived, for a live one), then its body split on
/// newlines with each line sanitised on its own, so a multi-line body keeps
/// its shape without carrying an escape, a NUL, or a bidi override into the
/// terminal (hard constraint 5).
///
/// No styling is added here — a body is prose from another person or agent,
/// not this build's own report, so there is nothing worth dimming, and
/// nothing here needs `render::styling_allowed`: the safest way to respect
/// `NO_COLOR` is to never emit color in the first place.
fn render_message(
    seq: RoomSeq,
    from: &HostId,
    kind: &MessageKind,
    body: &Body,
    at: Option<Timestamp>,
) -> String {
    let host = render::sanitize(from.as_str(), MAX_HOST_RENDER_CHARS);
    let kind = word(kind);
    let mut out = match at {
        Some(at) => format!("#{}  {host}  {kind}  {at}", seq.0),
        None => format!("#{}  {host}  {kind}", seq.0),
    };
    for line in body.as_str().lines() {
        let clean = render::sanitize(line, MAX_BODY_CHARS);
        out.push_str("\n    ");
        out.push_str(&clean);
    }
    out
}
#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use std::{
        collections::VecDeque,
        os::unix::fs::DirBuilderExt as _,
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use sealmesh_chat_wire::{Body, MessageKind, RoomId, Scope};
    use sealmesh_proto::{BackfillRefusal, HistoryEntry, RoomList};
    use sealmesh_wire::{HostId, ProjectId};
    use tokio::{io::AsyncReadExt as _, io::AsyncWriteExt as _, net::UnixListener};

    use super::*;
    use crate::room::local::{self, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, ResolvedRoom};

    fn scratch(tag: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("awchat{}{tag}", std::process::id() % 100_000));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let dir = root.join("local");
        std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
        dir.join("daemon.sock")
    }

    fn cleanup(path: &Path) {
        if let Some(root) = path.parent().and_then(Path::parent) {
            let _ = std::fs::remove_dir_all(root);
        }
    }

    /// A raw scripted answer: either an ordinary `Response`, or bytes sent
    /// exactly as given — the only way to hand the client something that does
    /// not even decode, or a `Tail` batch with one malformed entry inside an
    /// otherwise well-formed answer.
    enum Scripted {
        Ok(Response),
        Raw(Vec<u8>),
    }

    struct FakeDaemon {
        path: PathBuf,
        requests: Arc<Mutex<Vec<Request>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for FakeDaemon {
        fn drop(&mut self) {
            self.task.abort();
            cleanup(&self.path);
        }
    }

    /// Reads one newline-delimited request off `stream`, looping until a
    /// newline shows up rather than assuming one `read` call carries the
    /// whole line.
    async fn read_request(stream: &mut tokio::net::UnixStream) -> Option<Request> {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = stream.read(&mut chunk).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.contains(&b'\n') {
                break;
            }
            if buf.len() > MAX_REQUEST_BYTES {
                return None;
            }
        }
        local::decode::<Request>(buf.trim_ascii_end(), MAX_REQUEST_BYTES).ok()
    }

    /// Serves exactly `script.len()` requests, one response per connection,
    /// in order, then stops accepting. What it decoded is kept in
    /// `requests`, so a test can check what was asked — a `Join` happened
    /// with the right holder, a `Leave` did — without guessing from the
    /// printed output alone.
    fn start(tag: &str, script: Vec<Scripted>) -> FakeDaemon {
        let path = scratch(tag);
        let listener = UnixListener::bind(&path).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests_task = requests.clone();
        let task = tokio::spawn(async move {
            let mut script = VecDeque::from(script);
            while let Some(item) = script.pop_front() {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                if let Some(request) = read_request(&mut stream).await {
                    requests_task.lock().unwrap().push(request);
                }
                let bytes = match item {
                    Scripted::Ok(response) => local::encode(&response, MAX_RESPONSE_BYTES).unwrap(),
                    Scripted::Raw(bytes) => bytes,
                };
                let _ = stream.write_all(&bytes).await;
                let _ = stream.shutdown().await;
            }
        });
        FakeDaemon {
            path,
            requests,
            task,
        }
    }

    fn requests_of(rig: &FakeDaemon) -> Vec<Request> {
        rig.requests.lock().unwrap().clone()
    }

    fn room(scope: Scope) -> RoomId {
        RoomId::new(ProjectId::try_from("example-repo").unwrap(), scope)
    }

    fn repo() -> RepoSlug {
        RepoSlug::try_from("example-org/example-repo").unwrap()
    }

    fn resolved(room: &RoomId) -> Response {
        Response::Resolved {
            rooms: vec![ResolvedRoom {
                room: room.clone(),
                join: None,
            }],
        }
    }

    fn one_delivered(room: &RoomId, seq: u64, body: &str) -> Delivered {
        Delivered {
            room: room.clone(),
            seq: RoomSeq(seq),
            from: HostId::try_from("shelter").unwrap(),
            kind: MessageKind::Claim,
            body: Body::new(body).unwrap(),
            received_at: jiff::Timestamp::now(),
        }
    }

    fn delivered(seq: u64, body: &str) -> Response {
        let target = room(Scope::Repo(repo()));
        let entry = one_delivered(&target, seq, body);
        Response::Tail {
            room: target,
            messages: vec![entry],
            cursor: TailCursor {
                generation: 1,
                ordinal: seq,
            },
            lost: 0,
            reset: false,
        }
    }

    async fn output(
        rig: &FakeDaemon,
        scope: Option<&str>,
        since: Option<u64>,
    ) -> (Result<()>, String) {
        let checkout = github_checkout(&rig.path);
        let mut buf = Vec::new();
        let result = run_at(
            &mut buf,
            &rig.path,
            &checkout,
            scope.map(str::to_owned),
            since,
        )
        .await;
        std::fs::remove_dir_all(&checkout).ok();
        (result, String::from_utf8(buf).unwrap())
    }

    /// A scratch checkout next to `socket` whose origin is on GitHub, for the
    /// scopes that read the current checkout's repo.
    fn github_checkout(socket: &Path) -> PathBuf {
        let dir = socket.with_extension("checkout");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        for args in [
            &["init", "--initial-branch=main", "."][..],
            &[
                "remote",
                "add",
                "origin",
                "https://github.com/example-org/example-repo.git",
            ],
        ] {
            let ok = std::process::Command::new("git")
                .args(args)
                .current_dir(&dir)
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            assert!(ok, "git {args:?} failed");
        }
        dir
    }

    #[tokio::test]
    async fn listing_prints_each_live_room() {
        let rooms = RoomList::new(vec![
            room(Scope::Repo(repo())),
            room(Scope::Ticket(TicketId::new("ENG", 62).unwrap())),
        ])
        .unwrap();
        let rig = start(
            "list",
            vec![Scripted::Ok(Response::Rooms { rooms, age_secs: 0 })],
        );
        let (result, out) = output(&rig, None, None).await;
        result.unwrap();
        assert!(
            out.contains("example-repo/example-org/example-repo"),
            "{out}"
        );
        assert!(out.contains("example-repo/ENG-62"), "{out}");
    }

    #[tokio::test]
    async fn an_empty_listing_says_so_rather_than_nothing() {
        let rig = start(
            "empty",
            vec![Scripted::Ok(Response::Rooms {
                rooms: RoomList::new(vec![]).unwrap(),
                age_secs: 0,
            })],
        );
        let (result, out) = output(&rig, None, None).await;
        result.unwrap();
        assert!(out.contains("no rooms are live"), "{out}");
    }

    #[tokio::test]
    async fn a_throttled_listing_shows_the_retry_time() {
        let rig = start(
            "throttle",
            vec![Scripted::Ok(Response::ListThrottled {
                retry_after_secs: 42,
            })],
        );
        let (result, out) = output(&rig, None, None).await;
        result.unwrap();
        assert!(out.contains("42s"), "{out}");
    }

    #[tokio::test]
    async fn a_refused_listing_is_distinguishable_from_throttled() {
        let rig = start(
            "refused-list",
            vec![Scripted::Ok(Response::RoomListRefused {})],
        );
        let (result, out) = output(&rig, None, None).await;
        result.unwrap();
        assert!(out.contains("already running"), "{out}");
    }

    #[tokio::test]
    async fn disabled_reports_clearly_and_is_not_treated_as_no_room() {
        let rig = start("disabled", vec![Scripted::Ok(Response::Disabled {})]);
        let (result, out) = output(&rig, Some("example-org/example-repo"), None).await;
        let error = result.unwrap_err().to_string();
        assert!(error.contains("disabled"), "{error}");
        assert!(!out.contains(NO_ROOM), "{out}");
    }

    #[tokio::test]
    async fn a_daemon_that_is_not_running_reports_clearly() {
        let path = scratch("not-running");
        let mut buf = Vec::new();
        // A repo scope names its own repo, so the checkout is never read.
        let result = run_at(
            &mut buf,
            &path,
            &path,
            Some("example-org/example-repo".to_owned()),
            None,
        )
        .await;
        let error = result.unwrap_err().to_string();
        assert!(error.contains("not running"), "{error}");
        cleanup(&path);
    }

    #[tokio::test]
    async fn a_repo_scope_that_resolves_to_nothing_prints_no_room_and_succeeds() {
        let rig = start(
            "no-room",
            vec![Scripted::Ok(Response::Resolved { rooms: vec![] })],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), None).await;
        result.unwrap();
        assert!(out.contains(NO_ROOM), "{out}");
    }

    #[tokio::test]
    async fn a_resolved_list_of_the_wrong_scope_kind_is_still_no_room() {
        // The daemon resolved something — just not a repo room, which is
        // what was asked for. Matching "the list is non-empty" instead of
        // "the list has a room of the right scope" would treat this as found.
        let ticket_room = room(Scope::Ticket(TicketId::new("ENG", 1).unwrap()));
        let rig = start(
            "wrong-kind",
            vec![Scripted::Ok(Response::Resolved {
                rooms: vec![ResolvedRoom {
                    room: ticket_room,
                    join: None,
                }],
            })],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), None).await;
        result.unwrap();
        assert!(out.contains(NO_ROOM), "{out}");
    }

    #[tokio::test]
    async fn a_ticket_scope_picks_its_own_ticket_among_several_resolved_rooms() {
        // Both entries are ticket rooms, so only comparing the ticket itself
        // — not merely the scope's kind — can tell them apart.
        let wanted = TicketId::new("ENG", 7).unwrap();
        let decoy = TicketId::new("ENG", 8).unwrap();
        let target = room(Scope::Ticket(wanted.clone()));
        let other = room(Scope::Ticket(decoy));
        let rig = start(
            "ticket-pick",
            vec![
                Scripted::Ok(Response::Resolved {
                    rooms: vec![
                        ResolvedRoom {
                            room: other,
                            join: None,
                        },
                        ResolvedRoom {
                            room: target.clone(),
                            join: None,
                        },
                    ],
                }),
                Scripted::Ok(Response::Joined {
                    room: target.clone(),
                }),
                Scripted::Ok(delivered(1, "hello")),
                Scripted::Ok(Response::Disabled {}),
                Scripted::Ok(Response::Left {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("ENG-7"), None).await;
        assert!(result.is_err(), "{out}");
        assert!(out.contains("hello"), "{out}");
        let asked = requests_of(&rig);
        assert!(
            asked.iter().any(|r| matches!(
                r,
                Request::Tail { room, .. } if room == &target
            )),
            "must tail the wanted ticket's room, not the decoy: {asked:?}"
        );
    }

    #[tokio::test]
    async fn a_refused_join_prints_the_same_line_as_no_room() {
        let target = room(Scope::Repo(repo()));
        let rig = start(
            "refused-join",
            vec![
                Scripted::Ok(resolved(&target)),
                Scripted::Ok(Response::JoinRefused {
                    room: target.clone(),
                }),
                // Read-only cleanup is still attempted (harmless no-op).
                Scripted::Ok(Response::NotMember {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), None).await;
        result.unwrap();
        assert!(out.contains(NO_ROOM), "{out}");
    }

    #[tokio::test]
    async fn chat_joins_and_leaves_with_its_own_holder() {
        // Rule fix (XYZ-65 follow-up): no `Status` pre-check any more —
        // chat always joins and leaves under its own claim, and it is the
        // daemon's job, not chat's, to decide whether that actually leaves
        // the room (see room::client and room::inbox for that behaviour).
        let target = room(Scope::Repo(repo()));
        let rig = start(
            "own-holder",
            vec![
                Scripted::Ok(resolved(&target)),
                Scripted::Ok(Response::Joined {
                    room: target.clone(),
                }),
                Scripted::Ok(delivered(1, "hello")),
                Scripted::Ok(Response::Disabled {}),
                Scripted::Ok(Response::Left {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), None).await;
        assert!(result.is_err(), "{out}");
        assert!(out.contains("hello"), "{out}");
        assert!(
            !asked_status(&rig),
            "chat must not ask for `Status` before joining any more"
        );
        let expected = chat_holder().unwrap();
        let asked = requests_of(&rig);
        assert!(
            asked.iter().any(|r| matches!(
                r,
                Request::Join { room, holder } if room == &target && holder.as_ref() == Some(&expected)
            )),
            "{asked:?}"
        );
        assert!(
            asked.iter().any(|r| matches!(
                r,
                Request::Leave { room, holder } if room == &target && holder.as_ref() == Some(&expected)
            )),
            "{asked:?}"
        );
    }

    fn asked_status(rig: &FakeDaemon) -> bool {
        requests_of(rig)
            .iter()
            .any(|r| matches!(r, Request::Status {}))
    }

    #[tokio::test]
    async fn since_pages_through_backfill_before_tailing() {
        let target = room(Scope::Repo(repo()));
        let page1 = Response::Backfill {
            room: target.clone(),
            after: Some(RoomSeq(5)),
            messages: vec![HistoryEntry {
                seq: RoomSeq(6),
                from: HostId::try_from("shelter").unwrap(),
                kind: MessageKind::Claim,
                body: Body::new("page one").unwrap(),
            }],
            next: Some(RoomSeq(6)),
        };
        let page2 = Response::Backfill {
            room: target.clone(),
            after: Some(RoomSeq(6)),
            messages: vec![HistoryEntry {
                seq: RoomSeq(7),
                from: HostId::try_from("shelter").unwrap(),
                kind: MessageKind::Claim,
                body: Body::new("page two").unwrap(),
            }],
            next: None,
        };
        let rig = start(
            "pages",
            vec![
                Scripted::Ok(resolved(&target)),
                Scripted::Ok(Response::Joined {
                    room: target.clone(),
                }),
                Scripted::Ok(page1),
                Scripted::Ok(page2),
                Scripted::Ok(Response::Disabled {}),
                Scripted::Ok(Response::Left {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), Some(5)).await;
        assert!(result.is_err(), "{out}");
        let page_one_at = out.find("page one").expect(&out);
        let page_two_at = out.find("page two").expect(&out);
        assert!(page_one_at < page_two_at, "{out}");
        let asked = requests_of(&rig);
        let afters: Vec<_> = asked
            .iter()
            .filter_map(|r| match r {
                Request::Backfill { after, .. } => Some(*after),
                _ => None,
            })
            .collect();
        assert_eq!(
            afters,
            vec![Some(RoomSeq(5)), Some(RoomSeq(6))],
            "{asked:?}"
        );
    }

    #[tokio::test]
    async fn a_backfill_refusal_still_tails_afterwards() {
        let target = room(Scope::Repo(repo()));
        let rig = start(
            "backfill-refused",
            vec![
                Scripted::Ok(resolved(&target)),
                Scripted::Ok(Response::Joined {
                    room: target.clone(),
                }),
                Scripted::Ok(Response::BackfillRefused {
                    room: target.clone(),
                    category: local::Category::Known(BackfillRefusal::Unavailable),
                }),
                Scripted::Ok(delivered(9, "still tailing")),
                Scripted::Ok(Response::Disabled {}),
                Scripted::Ok(Response::Left {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), Some(1)).await;
        assert!(result.is_err(), "{out}");
        assert!(out.contains("unavailable"), "{out}");
        assert!(out.contains("still tailing"), "{out}");
    }

    #[tokio::test]
    async fn a_wholly_malformed_tail_answer_renders_in_place_and_the_room_still_renders() {
        let target = room(Scope::Repo(repo()));
        let rig = start(
            "malformed",
            vec![
                Scripted::Ok(resolved(&target)),
                Scripted::Ok(Response::Joined {
                    room: target.clone(),
                }),
                Scripted::Raw(b"not json at all\n".to_vec()),
                Scripted::Ok(delivered(1, "recovered")),
                Scripted::Ok(Response::Disabled {}),
                Scripted::Ok(Response::Left {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), None).await;
        assert!(result.is_err(), "{out}");
        assert!(out.contains("could not be read"), "{out}");
        assert!(out.contains("recovered"), "{out}");
    }

    #[tokio::test]
    async fn one_malformed_message_in_a_batch_renders_in_place_and_the_cursor_still_advances() {
        let target = room(Scope::Repo(repo()));
        let good1 = serde_json::to_value(one_delivered(&target, 1, "first")).unwrap();
        let mut bad = good1.clone();
        bad["kind"] = serde_json::json!("not_a_real_kind");
        let good3 = serde_json::to_value(one_delivered(&target, 3, "third")).unwrap();
        let batch = serde_json::json!({
            "result": "tail",
            "room": target,
            "messages": [good1, bad, good3],
            "cursor": {"generation": 1, "ordinal": 3},
            "lost": 0,
            "reset": false,
        });
        let raw = serde_json::to_vec(&batch).unwrap();
        let rig = start(
            "mixed-batch",
            vec![
                Scripted::Ok(resolved(&target)),
                Scripted::Ok(Response::Joined {
                    room: target.clone(),
                }),
                Scripted::Raw(raw),
                Scripted::Ok(Response::Disabled {}),
                Scripted::Ok(Response::Left {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), None).await;
        assert!(result.is_err(), "{out}");
        assert!(out.contains("first"), "{out}");
        assert!(out.contains("third"), "{out}");
        assert!(
            out.contains("one message in this batch could not be read"),
            "{out}"
        );
        let asked = requests_of(&rig);
        let afters: Vec<_> = asked
            .iter()
            .filter_map(|r| match r {
                Request::Tail { after, .. } => Some(*after),
                _ => None,
            })
            .collect();
        assert_eq!(
            afters,
            vec![
                None,
                Some(TailCursor {
                    generation: 1,
                    ordinal: 3
                })
            ],
            "the cursor must advance past the whole batch even though one message in it \
             could not be read: {asked:?}"
        );
    }

    #[tokio::test]
    async fn repeated_decode_failures_give_up_rather_than_looping_forever() {
        let target = room(Scope::Repo(repo()));
        let rig = start(
            "wedged",
            vec![
                Scripted::Ok(resolved(&target)),
                Scripted::Ok(Response::Joined {
                    room: target.clone(),
                }),
                Scripted::Raw(b"garbage one\n".to_vec()),
                Scripted::Raw(b"garbage two\n".to_vec()),
                Scripted::Raw(b"garbage three\n".to_vec()),
                Scripted::Ok(Response::Left {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), None).await;
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("older build") || error.contains("decode"),
            "{error}"
        );
        assert_eq!(out.matches("could not be read").count(), 3, "{out}");
    }

    #[tokio::test]
    async fn escape_and_bidi_bodies_are_neutralised_and_shape_is_kept() {
        let hostile = "line one\u{1b}[2J\nline two\u{202e}reversed";
        let target = room(Scope::Repo(repo()));
        let rig = start(
            "hostile-body",
            vec![
                Scripted::Ok(resolved(&target)),
                Scripted::Ok(Response::Joined {
                    room: target.clone(),
                }),
                Scripted::Ok(delivered(1, hostile)),
                Scripted::Ok(Response::Disabled {}),
                Scripted::Ok(Response::Left {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), None).await;
        assert!(result.is_err(), "{out}");
        assert!(!out.contains('\u{1b}'), "{out:?}");
        assert!(!out.contains('\u{202e}'), "{out:?}");
        assert!(out.contains("line one"), "{out}");
        assert!(out.contains("line two"), "{out}");
        assert!(out.contains("reversed"), "{out}");
    }

    #[tokio::test]
    async fn a_multiline_body_renders_each_line_on_its_own() {
        let target = room(Scope::Repo(repo()));
        let rig = start(
            "multiline",
            vec![
                Scripted::Ok(resolved(&target)),
                Scripted::Ok(Response::Joined {
                    room: target.clone(),
                }),
                Scripted::Ok(delivered(1, "line one\nline two\nline three")),
                Scripted::Ok(Response::Disabled {}),
                Scripted::Ok(Response::Left {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), None).await;
        assert!(result.is_err(), "{out}");
        let lines: Vec<&str> = out.lines().map(str::trim).collect();
        assert!(lines.contains(&"line one"), "{out}");
        assert!(lines.contains(&"line two"), "{out}");
        assert!(lines.contains(&"line three"), "{out}");
        assert!(
            !out.contains("line one line two"),
            "the body's lines were joined into one: {out}"
        );
    }

    #[tokio::test]
    async fn the_tail_cursor_advances_between_polls() {
        let target = room(Scope::Repo(repo()));
        let rig = start(
            "cursor-advances",
            vec![
                Scripted::Ok(resolved(&target)),
                Scripted::Ok(Response::Joined {
                    room: target.clone(),
                }),
                Scripted::Ok(delivered(1, "first")),
                Scripted::Ok(delivered(2, "second")),
                Scripted::Ok(Response::Disabled {}),
                Scripted::Ok(Response::Left {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), None).await;
        assert!(result.is_err(), "{out}");
        assert!(out.contains("first"), "{out}");
        assert!(out.contains("second"), "{out}");
        let asked = requests_of(&rig);
        let afters: Vec<_> = asked
            .iter()
            .filter_map(|r| match r {
                Request::Tail { after, .. } => Some(*after),
                _ => None,
            })
            .collect();
        assert_eq!(
            afters,
            vec![
                None,
                Some(TailCursor {
                    generation: 1,
                    ordinal: 1
                }),
                Some(TailCursor {
                    generation: 1,
                    ordinal: 2
                }),
            ],
            "{asked:?}"
        );
    }

    #[tokio::test]
    async fn a_backfill_page_with_no_progress_stops_rather_than_looping() {
        let target = room(Scope::Repo(repo()));
        let stuck = Response::Backfill {
            room: target.clone(),
            after: Some(RoomSeq(5)),
            messages: vec![],
            next: Some(RoomSeq(5)),
        };
        let rig = start(
            "stuck-backfill",
            vec![
                Scripted::Ok(resolved(&target)),
                Scripted::Ok(Response::Joined {
                    room: target.clone(),
                }),
                Scripted::Ok(stuck),
                Scripted::Ok(Response::Disabled {}),
                Scripted::Ok(Response::Left {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), Some(5)).await;
        assert!(result.is_err(), "{out}");
        assert!(out.contains("without making progress"), "{out}");
        let asked = requests_of(&rig);
        let backfills = asked
            .iter()
            .filter(|r| matches!(r, Request::Backfill { .. }))
            .count();
        assert_eq!(
            backfills, 1,
            "a page with no progress must not be re-asked: {asked:?}"
        );
    }

    #[tokio::test]
    async fn a_backfill_page_that_goes_backwards_also_stops_rather_than_looping() {
        // Fix: `next != after` would have accepted a cursor moving
        // backwards or oscillating; only `next > after` is forward progress.
        let target = room(Scope::Repo(repo()));
        let backwards = Response::Backfill {
            room: target.clone(),
            after: Some(RoomSeq(5)),
            messages: vec![],
            next: Some(RoomSeq(4)),
        };
        let rig = start(
            "backwards-backfill",
            vec![
                Scripted::Ok(resolved(&target)),
                Scripted::Ok(Response::Joined {
                    room: target.clone(),
                }),
                Scripted::Ok(backwards),
                Scripted::Ok(Response::Disabled {}),
                Scripted::Ok(Response::Left {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), Some(5)).await;
        assert!(result.is_err(), "{out}");
        assert!(out.contains("without making progress"), "{out}");
        let asked = requests_of(&rig);
        let backfills = asked
            .iter()
            .filter(|r| matches!(r, Request::Backfill { .. }))
            .count();
        assert_eq!(backfills, 1, "{asked:?}");
    }

    #[tokio::test]
    async fn a_daemon_at_its_room_limit_reports_it_plainly() {
        let target = room(Scope::Repo(repo()));
        let rig = start(
            "room-limit",
            vec![
                Scripted::Ok(resolved(&target)),
                Scripted::Ok(Response::RoomLimit {}),
                Scripted::Ok(Response::NotMember {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), None).await;
        let error = result.unwrap_err().to_string();
        assert!(error.contains("holds as many rooms"), "{error}");
        assert!(!out.contains(NO_ROOM), "{out}");
    }

    #[tokio::test]
    async fn an_invalid_answer_is_reported_as_an_old_daemon() {
        let target = room(Scope::Repo(repo()));
        let rig = start(
            "invalid",
            vec![
                Scripted::Ok(resolved(&target)),
                Scripted::Ok(Response::Invalid {}),
                Scripted::Ok(Response::NotMember {
                    room: target.clone(),
                }),
            ],
        );
        let (result, out) = output(&rig, Some("example-org/example-repo"), None).await;
        let error = result.unwrap_err().to_string();
        assert!(error.contains("older build"), "{error}");
        assert!(!out.contains(NO_ROOM), "{out}");
    }

    #[tokio::test]
    async fn a_signal_set_between_two_calls_is_still_caught_by_the_same_shutdown() {
        // The property `race` depends on: one `Shutdown`, threaded through
        // by `&mut` reference across an entire `tail` call, must notice a
        // signal that arrived strictly between two of its uses — not only
        // one that arrives while a particular `race` call is in flight.
        // This would break if some future change recreated a fresh
        // `Shutdown` (or a fresh underlying receiver) per poll instead of
        // reusing the one passed in.
        let (tx, rx) = tokio::sync::watch::channel(false);
        let mut signals = Shutdown::from_watch(rx);

        let first = race(&mut signals, async { 1u32 }).await;
        assert_eq!(first, Some(1), "nothing has signalled yet");

        // The signal lands here, strictly between two calls on the same
        // `Shutdown` — not while either call is itself in flight.
        let _ = tx.send(true);

        let second = race(&mut signals, std::future::pending::<u32>()).await;
        assert_eq!(
            second, None,
            "a signal that landed between two calls on the one `Shutdown` a whole `tail` call \
             shares must still be caught by the next one"
        );
    }

    #[test]
    fn scope_parsing_covers_the_three_forms_and_refuses_the_rest() {
        assert_eq!(parse_scope("project").unwrap(), ChatScope::Project);
        assert_eq!(parse_scope("PROJECT").unwrap(), ChatScope::Project);
        assert_eq!(
            parse_scope("example-org/example-repo").unwrap(),
            ChatScope::Repo(repo())
        );
        assert_eq!(
            parse_scope("XYZ-62").unwrap(),
            ChatScope::Ticket(TicketId::new("XYZ", 62).unwrap())
        );
        assert_eq!(
            parse_scope("xyz-62").unwrap(),
            ChatScope::Ticket(TicketId::new("XYZ", 62).unwrap())
        );
        for bad in ["", "not a scope", "a/b/c", "xyz", "123-456"] {
            assert!(parse_scope(bad).is_err(), "accepted {bad:?}");
        }
    }
}
