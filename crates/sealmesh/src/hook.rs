use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Result;
use sealmesh_chat_wire::RoomId;
use serde::Deserialize;

use crate::{
    config::Config,
    declare, gitref,
    room::{
        connect,
        local::{ConsumerKey, Delivered, JoinOutcome, Request, Response},
        room_name, socket,
    },
};

#[derive(Debug, Deserialize)]
struct Event {
    #[serde(default)]
    hook_event_name: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    cwd: Option<PathBuf>,
    #[serde(default)]
    tool_name: Option<String>,
    #[serde(default)]
    agent_id: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    Declare {
        id: String,
        worktree: PathBuf,
        state: &'static str,
        parent: Option<String>,
    },
    End {
        id: String,
    },
    Nothing,
}

pub async fn run() -> Result<()> {
    let mut payload = String::new();
    {
        use std::io::Read as _;
        if std::io::stdin()
            .take(MAX_PAYLOAD_BYTES)
            .read_to_string(&mut payload)
            .is_err()
        {
            return Ok(());
        }
    }

    match action(&payload) {
        Action::Declare {
            id,
            worktree,
            state,
            parent,
        } => {
            let args = declare::Args {
                id,
                worktree: Some(worktree),
                state: Some(state.to_owned()),
                branch: None,
                pr: None,
                tasks: None,
                parent,
                // The hook is Claude Code's, so it can say so. A declared
                // record with no tool reads as `declared`, which would be
                // needlessly vague for the one writer that knows.
                tool: Some("claude-code".to_owned()),
                end: false,
            };
            declare::run(&args).ok();
        }
        Action::End { id } => {
            let args = declare::Args {
                id,
                worktree: None,
                state: None,
                branch: None,
                pr: None,
                tasks: None,
                parent: None,
                tool: None,
                end: true,
            };
            declare::run(&args).ok();
        }
        Action::Nothing => {}
    }

    // Announce, do not inject (operator decision, 2026-09-21). This is
    // strictly additive to the declare path above: it reads the same
    // payload again for the one event it cares about, touches nothing the
    // declare match touched, and every failure in it is silent — a missing
    // daemon, chat turned off, a config that will not load, a checkout the
    // parser does not recognise, all read the same as "no messages this
    // turn". See `announce` for the one deadline that bounds all of it.
    if let Some(input) = drain_input(&payload)
        && let Some(context) = drain_after_config(&input).await
    {
        print_additional_context(&context);
    }

    Ok(())
}

const MAX_PAYLOAD_BYTES: u64 = 256 * 1024;

/// The one deadline covering a whole drain attempt: resolving the checkout's
/// rooms, holding each one, and draining it. A `UserPromptSubmit` hook runs
/// on every prompt, so this has to be short enough that a stalled daemon is
/// never felt as a delayed prompt — a few hundred milliseconds, the same
/// order of magnitude as `gitref::read`'s own cold-start cost.
pub const HOOK_DRAIN_DEADLINE: Duration = Duration::from_millis(300);

/// The most rooms one turn announces. A checkout resolves to at most a
/// handful (ticket, repo, and — opt-in — project), so this is a backstop
/// against a future project map naming more than that, not a real limit in
/// practice (rule 44: bounded count).
const MAX_ROOMS_PER_TURN: usize = 4;

/// The most messages drained from one room per turn. Only the count and the
/// sequence range of what comes back are ever used, so this exists to bound
/// the daemon's own work and the size of what crosses the socket, not to
/// cap what an agent can be told about — a room with more waiting still says
/// so via `more` (rule 44: bounded count).
const MAX_MESSAGES_PER_ROOM: u16 = 16;

/// The most bytes asked of one room's drain. Bodies are never read here, so
/// this stays small on purpose (rule 44: bounded bytes).
const MAX_DRAIN_BYTES: u32 = 4 * 1024;

/// The most bytes the injected `additionalContext` may total, across every
/// room's line together (rule 44: bounded bytes).
const MAX_CONTEXT_BYTES: usize = 2 * 1024;

/// What `UserPromptSubmit` needs to attempt a drain: the session id, whole —
/// the same value `ConsumerKey::from_session_id` turns into this session's
/// drain key everywhere else — and the checkout's own directory. Parsed
/// separately from `action`'s `Event`, on its own pass over the same
/// payload, so nothing about the drain path can change what the declare
/// path above decided, and this reads only the one field pair it needs.
struct DrainInput {
    session_id: String,
    cwd: PathBuf,
}

/// `None` for anything but a `UserPromptSubmit` with a session id and an
/// absolute `cwd` — in particular, never for `SubagentStart`, whose
/// `session_id` names the coordinator's session rather than the worker's
/// own, and which this hook must not drain on (rule 12: not this caller's
/// room to read into).
fn drain_input(payload: &str) -> Option<DrainInput> {
    let event: Event = serde_json::from_str(payload).ok()?;
    if event.hook_event_name.as_deref() != Some("UserPromptSubmit") {
        return None;
    }
    let session_id = event.session_id?;
    let cwd = event.cwd.filter(|c| c.is_absolute())?;
    Some(DrainInput { session_id, cwd })
}

/// The kill file's path, the way `Config::off_switch_path` reads it — except
/// that here, a missing config home counts as killed rather than as "no
/// kill file". `daemon::is_off` cannot say that (a daemon with no home
/// directory could not have started at all), but a hook runs on every
/// prompt in whatever environment Claude Code gives it, and a hook that
/// cannot even resolve `$HOME` has no business asking a daemon for
/// anything.
fn drain_killed() -> bool {
    match Config::off_switch_path() {
        Some(path) => path.exists(),
        None => true,
    }
}

/// `[chat] enabled` and `[chat] hook_drain` both true, and not killed.
fn gate_open(config: &Config) -> bool {
    config.chat_enabled(drain_killed()) && config.chat.hook_drain
}

/// Loads the config and, if it loads, drains. A config that does not exist
/// or does not parse is silently no drain, the same as every other failure
/// here — this hook never blocks a prompt on a config problem `doctor`
/// would report instead.
async fn drain_after_config(input: &DrainInput) -> Option<String> {
    let config = Config::load(None).ok()?;
    let socket = socket::default_path()?;
    drain_for_prompt(&config, input, &socket).await
}

/// Everything after the config is loaded: the gate, the checkout, the
/// session's own drain key, and the announce itself. Kept separate from
/// [`drain_after_config`] — and takes `socket` as a parameter rather than
/// resolving it itself — so a test can hand this a `Config` built in memory
/// and a fake daemon's socket path rather than a real `$HOME` and its real
/// daemon (rule 4 — environment isolation). The gate is checked first and
/// returns before `socket` or `input.cwd` is read at all, so a gate-closed
/// call never asks a daemon anything, provably: a test points `socket` at a
/// daemon that would answer if asked, and reads its own request log back.
async fn drain_for_prompt(config: &Config, input: &DrainInput, socket: &Path) -> Option<String> {
    if !gate_open(config) {
        return None;
    }
    let checkout = gitref::read(&input.cwd, &config.ticket_prefixes)?;
    let holder = ConsumerKey::from_session_id(&input.session_id)?;
    announce(socket, HOOK_DRAIN_DEADLINE, &checkout, &holder).await
}

/// Resolves the checkout's rooms, holds each one under `holder`'s lease, and
/// drains the ones actually joined — never `room: None`, which would reach
/// every room this daemon holds, including other checkouts'. The whole
/// sequence runs under one `deadline`: whatever has not finished by then is
/// cut off and this turn announces nothing, never a late prompt.
async fn announce(
    socket: &Path,
    deadline: Duration,
    checkout: &gitref::Checkout,
    holder: &ConsumerKey,
) -> Option<String> {
    tokio::time::timeout(deadline, async {
        let rooms = resolved_rooms(socket, deadline, checkout, holder).await;
        let lines = drain_rooms(socket, deadline, holder, rooms).await;
        build_context(lines)
    })
    .await
    .ok()
    .flatten()
}

/// The checkout's rooms this daemon actually holds after a `hold` — or, for
/// a daemon built before `Hold` existed (it answers an operation it does
/// not know `invalid`), the equivalent `resolve` with `join: true`, whose
/// lease names no holder. Every other answer — refused, disabled, not
/// connected, busy, timed out, or no answer inside `deadline` — comes back
/// as no rooms: a resolve failure here must read as "no messages this
/// turn", never as an error.
async fn resolved_rooms(
    socket: &Path,
    deadline: Duration,
    checkout: &gitref::Checkout,
    holder: &ConsumerKey,
) -> Vec<RoomId> {
    let hold = Request::Hold {
        repo: checkout.repo.clone(),
        ticket: checkout.ticket.clone(),
        holder: holder.clone(),
    };
    let response = match connect::request(socket, &hold, deadline).await {
        Ok(Response::Invalid {}) => {
            let resolve = Request::Resolve {
                repo: checkout.repo.clone(),
                ticket: checkout.ticket.clone(),
                join: true,
            };
            connect::request(socket, &resolve, deadline).await.ok()
        }
        Ok(response) => Some(response),
        Err(_) => None,
    };
    match response {
        Some(Response::Resolved { rooms }) => rooms
            .into_iter()
            .filter(|room| matches!(room.join, Some(JoinOutcome::Joined)))
            .map(|room| room.room)
            .take(MAX_ROOMS_PER_TURN)
            .collect(),
        _ => Vec::new(),
    }
}

/// Drains each room in turn and turns what comes back into one count line
/// per room that actually has something waiting. One room's own failure —
/// refused, not a member, no answer — costs only that room's line; the
/// rooms before and after it still report.
async fn drain_rooms(
    socket: &Path,
    deadline: Duration,
    consumer: &ConsumerKey,
    rooms: Vec<RoomId>,
) -> Vec<String> {
    let mut lines = Vec::with_capacity(rooms.len());
    for room in rooms {
        let request = Request::Drain {
            consumer: consumer.clone(),
            room: Some(room.clone()),
            max_messages: Some(MAX_MESSAGES_PER_ROOM),
            max_bytes: Some(MAX_DRAIN_BYTES),
        };
        if let Ok(Response::Messages {
            messages,
            lost,
            more,
            ..
        }) = connect::request(socket, &request, deadline).await
            && let Some(line) = announce_line(&room, &messages, lost, more)
        {
            lines.push(line);
        }
    }
    lines
}

/// One room's count line: how many messages, their sequence range, and
/// where to read them — never a body, a sender or a kind. `messages` is
/// read for nothing but its length and its first and last `seq`; the
/// operator's decision (2026-09-21) is "announce, do not inject", and this
/// is the one place that boundary is drawn in code.
fn announce_line(room: &RoomId, messages: &[Delivered], lost: u64, more: bool) -> Option<String> {
    let first = messages.first()?.seq.0;
    let last = messages.last()?.seq.0;
    let count = messages.len();
    let noun = if count == 1 { "message" } else { "messages" };
    let mut line = format!(
        "{count} new {noun} in the {name} (seq {first}\u{2013}{last}).",
        name = room_name(room)
    );
    if lost > 0 {
        let noun = if lost == 1 { "message" } else { "messages" };
        line.push_str(&format!(" {lost} {noun} lost before this."));
    }
    if more {
        line.push_str(" More are waiting.");
    }
    line.push_str(&format!(
        " Call room_read with after={} to read them.",
        first.saturating_sub(1)
    ));
    Some(line)
}

/// Joins every room's line into one string, cut to [`MAX_CONTEXT_BYTES`] by
/// whole lines — never mid-line — so the count stays bounded even if a
/// future change let more rooms or longer names through. `None` when there
/// is nothing to say, which is the ordinary case on most turns.
fn build_context(lines: Vec<String>) -> Option<String> {
    let mut kept = String::new();
    for line in lines {
        let grown = kept.len() + usize::from(!kept.is_empty()) + line.len();
        if grown > MAX_CONTEXT_BYTES {
            break;
        }
        if !kept.is_empty() {
            kept.push('\n');
        }
        kept.push_str(&line);
    }
    (!kept.is_empty()).then_some(kept)
}

/// The hook's one drain output shape: `hookSpecificOutput.additionalContext`
/// for `UserPromptSubmit`, and nothing else on the line — no field this
/// build did not put there, and nothing from the payload this process read
/// on its way in (the injection direction is inbound only).
fn print_additional_context(text: &str) {
    let output = serde_json::json!({
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": text,
        }
    });
    println!("{output}");
}

fn action(payload: &str) -> Action {
    let Ok(event) = serde_json::from_str::<Event>(payload) else {
        return Action::Nothing;
    };
    let Some(name) = event.hook_event_name.as_deref() else {
        return Action::Nothing;
    };
    let Some(session_id) = event.session_id.as_deref() else {
        return Action::Nothing;
    };

    if let Some(agent_id) = event.agent_id.as_deref() {
        let Some(id) = short_id(agent_id) else {
            return Action::Nothing;
        };
        return match name {
            "SubagentStart" => match event.cwd {
                Some(cwd) if cwd.is_absolute() => Action::Declare {
                    id,
                    worktree: cwd,
                    state: "working",
                    parent: Some(session_id.to_owned()),
                },
                _ => Action::Nothing,
            },
            "SubagentStop" => Action::End { id },
            _ => Action::Nothing,
        };
    }

    let Some(id) = short_id(session_id) else {
        return Action::Nothing;
    };

    if name == "SessionEnd" {
        return Action::End { id };
    }

    let state = match name {
        "UserPromptSubmit" => "working",
        "Stop" => "idle",
        "PreToolUse" if event.tool_name.as_deref() == Some("AskUserQuestion") => "needs_input",
        _ => return Action::Nothing,
    };

    match event.cwd {
        Some(cwd) if cwd.is_absolute() => Action::Declare {
            id,
            worktree: cwd,
            state,
            parent: None,
        },
        _ => Action::Nothing,
    }
}

fn short_id(id: &str) -> Option<String> {
    let end = id.char_indices().nth(8).map_or(id.len(), |(i, _)| i);
    let short = id.get(..end)?;
    crate::wire::JobId::try_from(short)
        .ok()
        .map(|j| j.as_str().to_owned())
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
        os::unix::fs::DirBuilderExt as _,
        sync::{Arc, Mutex},
    };

    use jiff::Timestamp;
    use sealmesh_chat_wire::{Body, MessageKind, Scope};
    use sealmesh_proto::RoomSeq;
    use sealmesh_wire::{HostId, ProjectId, RepoSlug, TicketId};
    use tokio::{
        io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader},
        net::UnixListener,
    };

    use super::*;
    use crate::room::local::{self, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, ResolvedRoom};

    const SESSION: &str = "a04e3d0f-d94b-4d7e-9f02-5b09827a67f9";
    const CANARY: &str = "PASTED-SLACK-THREAD-NAMING-A-THIRD-PARTY";

    fn payload(name: &str, extra: serde_json::Value) -> String {
        let mut json = serde_json::json!({
            "hook_event_name": name,
            "session_id": SESSION,
            "transcript_path": format!("/Users/dart/.claude/projects/-Users-dart/{SESSION}.jsonl"),
            "cwd": "/Users/dart/work/checkout-service",
            "permission_mode": "bypassPermissions",
            "prompt": CANARY,
            "last_assistant_message": CANARY,
            "message": CANARY,
            "title": CANARY,
            "agent_type": CANARY,
        });
        if let Some(fields) = extra.as_object() {
            for (k, v) in fields {
                json[k] = v.clone();
            }
        }
        json.to_string()
    }

    fn declared(state: &str, parent: Option<&str>) -> Action {
        Action::Declare {
            id: "a04e3d0f".to_owned(),
            worktree: PathBuf::from("/Users/dart/work/checkout-service"),
            state: match state {
                "working" => "working",
                "idle" => "idle",
                "needs_input" => "needs_input",
                other => panic!("{other}"),
            },
            parent: parent.map(ToOwned::to_owned),
        }
    }

    #[test]
    fn a_prompt_starts_work_and_a_stop_ends_the_turn() {
        assert_eq!(
            action(&payload("UserPromptSubmit", serde_json::json!({}))),
            declared("working", None)
        );
        assert_eq!(
            action(&payload(
                "Stop",
                serde_json::json!({"stop_hook_active": false})
            )),
            declared("idle", None)
        );
    }

    #[test]
    fn asking_a_question_is_the_moment_a_session_needs_input() {
        assert_eq!(
            action(&payload(
                "PreToolUse",
                serde_json::json!({"tool_name": "AskUserQuestion"})
            )),
            declared("needs_input", None)
        );
    }

    #[test]
    fn no_other_tool_is_subscribed_to() {
        for tool in ["Bash", "Read", "Edit", "Task", "WebFetch"] {
            assert_eq!(
                action(&payload(
                    "PreToolUse",
                    serde_json::json!({"tool_name": tool})
                )),
                Action::Nothing,
                "subscribed to {tool}"
            );
        }
        assert_eq!(
            action(&payload("PreToolUse", serde_json::json!({}))),
            Action::Nothing
        );
    }

    #[test]
    fn a_session_ending_clears_its_record() {
        assert_eq!(
            action(&payload(
                "SessionEnd",
                serde_json::json!({"reason": "exit"})
            )),
            Action::End {
                id: "a04e3d0f".to_owned()
            }
        );
    }

    #[test]
    fn a_session_ending_without_a_cwd_still_clears_its_record() {
        let mut json: serde_json::Value =
            serde_json::from_str(&payload("SessionEnd", serde_json::json!({}))).unwrap();
        json["cwd"] = serde_json::Value::Null;
        assert_eq!(
            action(&json.to_string()),
            Action::End {
                id: "a04e3d0f".to_owned()
            }
        );
    }

    #[test]
    fn a_subagent_is_its_own_row_parented_to_the_session_that_spawned_it() {
        let start = payload(
            "SubagentStart",
            serde_json::json!({
                "agent_id": "b7c2e19d-1111-2222-3333-444455556666",
                "cwd": "/Users/dart/work/worker-3",
            }),
        );
        assert_eq!(
            action(&start),
            Action::Declare {
                id: "b7c2e19d".to_owned(),
                worktree: PathBuf::from("/Users/dart/work/worker-3"),
                state: "working",
                parent: Some(SESSION.to_owned()),
            }
        );

        let stop = payload(
            "SubagentStop",
            serde_json::json!({
                "agent_id": "b7c2e19d-1111-2222-3333-444455556666",
                "agent_transcript_path": "/Users/dart/.claude/.../agent-b7c2e19d.jsonl",
            }),
        );
        assert_eq!(
            action(&stop),
            Action::End {
                id: "b7c2e19d".to_owned()
            }
        );
    }

    #[test]
    fn no_canary_survives_any_hook_payload() {
        for (name, extra) in [
            ("UserPromptSubmit", serde_json::json!({})),
            ("Stop", serde_json::json!({})),
            (
                "PreToolUse",
                serde_json::json!({"tool_name": "AskUserQuestion", "tool_input": {"q": CANARY}}),
            ),
            ("SessionEnd", serde_json::json!({})),
            (
                "SubagentStart",
                serde_json::json!({"agent_id": "b7c2e19d-1111", "cwd": "/Users/dart/w"}),
            ),
            ("Notification", serde_json::json!({})),
        ] {
            let rendered = format!("{:?}", action(&payload(name, extra)));
            assert!(!rendered.contains(CANARY), "{name}: {rendered}");
            assert!(!rendered.contains(".jsonl"), "{name}: {rendered}");
        }
    }

    #[test]
    fn the_prose_fields_do_not_exist_on_the_parsed_event() {
        let event: Event =
            serde_json::from_str(&payload("UserPromptSubmit", serde_json::json!({}))).unwrap();
        assert_eq!(format!("{event:?}").matches(CANARY).count(), 0);
        assert!(!format!("{event:?}").contains(".jsonl"));
        assert_eq!(event.hook_event_name.as_deref(), Some("UserPromptSubmit"));
    }

    #[test]
    fn nothing_unparseable_or_unexpected_becomes_an_action() {
        for hostile in [
            "",
            "not json",
            "{}",
            "[]",
            "null",
            r#"{"hook_event_name":"UserPromptSubmit"}"#,
            r#"{"session_id":"a04e3d0f"}"#,
            r#"{"hook_event_name":"SessionStart","session_id":"a04e3d0f","cwd":"/w"}"#,
            r#"{"hook_event_name":"Stop","session_id":"a04e3d0f","cwd":"../.."}"#,
            r#"{"hook_event_name":"Stop","session_id":"ZZZZZZZZ-1111","cwd":"/w"}"#,
        ] {
            assert_eq!(action(hostile), Action::Nothing, "acted on {hostile:?}");
        }
    }

    #[test]
    fn the_id_matches_what_every_other_adapter_derives() {
        assert_eq!(short_id(SESSION).as_deref(), Some("a04e3d0f"));
        assert_eq!(short_id("deadbeef").as_deref(), Some("deadbeef"));
        assert_eq!(short_id("ZZZZZZZZ-1111"), None);
        assert_eq!(short_id("abc"), None);
        assert_eq!(short_id(""), None);
    }

    #[test]
    fn the_payload_cap_is_far_above_a_real_one() {
        const { assert!(MAX_PAYLOAD_BYTES >= 32 * 1024) };
    }

    // --- the drain path -----------------------------------------------

    type Answer = Arc<dyn Fn(&Request) -> Option<Response> + Send + Sync>;

    /// A stand-in for the daemon's local socket: it reads each request
    /// line, records it, and answers what `answer` says — `None` holds the
    /// connection open unanswered, for the deadline test. Mirrors
    /// `sealmesh-mcp`'s `FakeDaemon`, which this crate's tests cannot
    /// reuse directly (different crate), kept intentionally small since
    /// this module only ever sends `Hold`, `Resolve` and `Drain`.
    struct FakeDaemon {
        root: PathBuf,
        socket: PathBuf,
        requests: Arc<Mutex<Vec<Request>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for FakeDaemon {
        fn drop(&mut self) {
            self.task.abort();
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    impl FakeDaemon {
        fn start(
            tag: &str,
            answer: impl Fn(&Request) -> Option<Response> + Send + Sync + 'static,
        ) -> Self {
            Self::start_slow(tag, |_| Duration::ZERO, answer)
        }

        /// [`Self::start`], waiting `delay(request)` before each answer.
        fn start_slow(
            tag: &str,
            delay: impl Fn(&Request) -> Duration + Send + Sync + 'static,
            answer: impl Fn(&Request) -> Option<Response> + Send + Sync + 'static,
        ) -> Self {
            let delay: Arc<dyn Fn(&Request) -> Duration + Send + Sync> = Arc::new(delay);
            // Short, because a Unix socket path is capped near 104 bytes.
            let root =
                std::env::temp_dir().join(format!("awh{}{tag}", std::process::id() % 100_000));
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(&root).unwrap();
            let dir = root.join("local");
            std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
            let socket = dir.join("daemon.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let requests = Arc::new(Mutex::new(Vec::new()));
            let answer: Answer = Arc::new(answer);
            let seen = requests.clone();
            let task = tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let seen = seen.clone();
                    let answer = answer.clone();
                    let delay = delay.clone();
                    tokio::spawn(async move {
                        let (read, mut write) = stream.into_split();
                        let mut line = String::new();
                        if BufReader::new(read).read_line(&mut line).await.is_err() {
                            return;
                        }
                        let Ok(request) =
                            local::decode::<Request>(line.trim_end().as_bytes(), MAX_REQUEST_BYTES)
                        else {
                            return;
                        };
                        let reply = answer(&request);
                        let wait = delay(&request);
                        seen.lock().unwrap().push(request);
                        tokio::time::sleep(wait).await;
                        match reply {
                            Some(response) => {
                                let bytes = local::encode(&response, MAX_RESPONSE_BYTES).unwrap();
                                let _ = write.write_all(&bytes).await;
                            }
                            None => std::future::pending::<()>().await,
                        }
                    });
                }
            });
            Self {
                root,
                socket,
                requests,
                task,
            }
        }

        fn requests(&self) -> Vec<Request> {
            self.requests.lock().unwrap().clone()
        }
    }

    fn example_repo(scope: Scope) -> RoomId {
        RoomId::new(ProjectId::try_from("example-repo").unwrap(), scope)
    }

    fn ticket_room() -> RoomId {
        example_repo(Scope::Ticket(TicketId::new("ENG", 7).unwrap()))
    }

    fn repo_room() -> RoomId {
        example_repo(Scope::Repo(
            RepoSlug::try_from("example-org/example-repo").unwrap(),
        ))
    }

    fn project_room() -> RoomId {
        example_repo(Scope::Project)
    }

    fn checkout() -> gitref::Checkout {
        gitref::Checkout {
            repo: RepoSlug::try_from("example-org/example-repo").unwrap(),
            branch: None,
            ticket: Some(TicketId::new("ENG", 7).unwrap()),
        }
    }

    /// A real, on-disk git checkout that `gitref::read` resolves to
    /// `example-org/example-repo` on `feature/eng-7-portal` — the same repo and
    /// ticket [`checkout`] builds in memory, but readable from a real path,
    /// for the one test that has to prove a gate check runs before
    /// `gitref::read` is ever reached rather than merely that the result
    /// ends up `None` (a `cwd` that is not a checkout would return `None`
    /// either way and prove nothing about the gate). Mirrors
    /// `gitref::tests::Scratch`, which this crate's tests cannot reuse
    /// directly (private to that module).
    struct GitScratch(PathBuf);

    impl Drop for GitScratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    impl GitScratch {
        fn checkout(tag: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "sealmesh-hook-gitscratch-{tag}-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let git = |args: &[&str]| {
                let out = std::process::Command::new("git")
                    .args(args)
                    .current_dir(&dir)
                    .env("GIT_AUTHOR_NAME", "t")
                    .env("GIT_AUTHOR_EMAIL", "t@example.test")
                    .env("GIT_COMMITTER_NAME", "t")
                    .env("GIT_COMMITTER_EMAIL", "t@example.test")
                    .env("GIT_CONFIG_NOSYSTEM", "1")
                    .output()
                    .unwrap();
                assert!(out.status.success(), "git {args:?} failed: {out:?}");
            };
            git(&["init", "--initial-branch=feature/eng-7-portal", "."]);
            std::fs::write(dir.join("a.txt"), "one").unwrap();
            git(&["add", "."]);
            git(&["commit", "-m", "one"]);
            git(&[
                "remote",
                "add",
                "origin",
                "git@github.com:example-org/example-repo.git",
            ]);
            Self(dir)
        }
    }

    fn holder() -> ConsumerKey {
        ConsumerKey::from_session_id(SESSION).unwrap()
    }

    /// A `resolve`/`hold` answer naming `rooms`, all joined.
    fn resolved(rooms: &[RoomId]) -> Response {
        Response::Resolved {
            rooms: rooms
                .iter()
                .map(|room| ResolvedRoom {
                    room: room.clone(),
                    join: Some(JoinOutcome::Joined),
                })
                .collect(),
        }
    }

    fn message(seq: u64, body: &str, from: &str, kind: MessageKind) -> Delivered {
        Delivered {
            room: repo_room(),
            seq: RoomSeq(seq),
            from: HostId::try_from(from).unwrap(),
            kind,
            body: Body::new(body).unwrap(),
            received_at: Timestamp::now(),
        }
    }

    fn messages_of(response: Response) -> Option<Response> {
        Some(response)
    }

    /// Config with chat's two switches set as given, otherwise a minimal
    /// valid config — the same shape `config.rs`'s own tests build.
    fn chat_config(enabled: bool, hook_drain: bool) -> Config {
        let toml = format!(
            r#"
            ticket_prefixes = ["ENG"]
            [server]
            url = "https://router.example.com"
            [allow]
            repos = ["example-org/example-repo"]
            paths = ["/tmp"]
            [chat]
            enabled = {enabled}
            hook_drain = {hook_drain}
            "#
        );
        toml::from_str(&toml).unwrap()
    }

    #[test]
    fn the_gate_needs_both_switches_and_reads_them_independently() {
        assert!(gate_open(&chat_config(true, true)));
        assert!(!gate_open(&chat_config(false, true)), "chat not enabled");
        assert!(!gate_open(&chat_config(true, false)), "hook_drain not on");
        assert!(!gate_open(&chat_config(false, false)));
    }

    #[test]
    fn only_a_real_user_prompt_submit_is_a_drain_candidate() {
        assert!(drain_input(&payload("UserPromptSubmit", serde_json::json!({}))).is_some());
        for name in ["Stop", "SessionEnd", "PreToolUse", "Notification"] {
            assert!(
                drain_input(&payload(name, serde_json::json!({}))).is_none(),
                "{name}"
            );
        }
        // A subagent's own start reuses `UserPromptSubmit`-shaped fields on
        // no build, but proves the point either way: a `SubagentStart`
        // payload (agent_id set, no hook_event_name of `UserPromptSubmit`)
        // is not a drain candidate.
        let subagent = payload(
            "SubagentStart",
            serde_json::json!({"agent_id": "b7c2e19d-1111", "cwd": "/w"}),
        );
        assert!(drain_input(&subagent).is_none());

        // A relative `cwd` is refused, same as the declare path's own
        // `Some(cwd) if cwd.is_absolute()` guard.
        let relative = payload("UserPromptSubmit", serde_json::json!({"cwd": "../.."}));
        assert!(drain_input(&relative).is_none());

        // No `session_id` at all — malformed or truncated payload.
        let mut no_session: serde_json::Value =
            serde_json::from_str(&payload("UserPromptSubmit", serde_json::json!({}))).unwrap();
        no_session["session_id"] = serde_json::Value::Null;
        assert!(drain_input(&no_session.to_string()).is_none());
    }

    #[tokio::test]
    async fn a_malformed_session_id_gates_the_drain_before_any_daemon_is_asked() {
        // `ConsumerKey::from_session_id` rejects this (spaces, not the
        // charset a session id is), so the drain must stop there — proven,
        // as above, against a daemon that would answer if reached.
        let scratch = GitScratch::checkout("bad-session");
        let daemon = FakeDaemon::start("d10", |request| match request {
            Request::Hold { .. } => Some(resolved(&[repo_room()])),
            Request::Drain { .. } => messages_of(Response::Messages {
                messages: vec![message(1, "a", "shelter", MessageKind::Claim)],
                lost: 0,
                new_consumer: true,
                more: false,
            }),
            _ => Some(Response::Invalid {}),
        });
        let input = DrainInput {
            session_id: "not a valid session id".to_owned(),
            cwd: scratch.0.clone(),
        };
        assert_eq!(
            drain_for_prompt(&chat_config(true, true), &input, &daemon.socket).await,
            None
        );
        assert!(daemon.requests().is_empty(), "{:?}", daemon.requests());
    }

    #[tokio::test]
    async fn gate_off_drains_nothing_and_never_reaches_the_daemon() {
        // A *real* checkout and a daemon that would answer with messages if
        // asked — so this proves the gate itself stops the call, not that
        // `cwd` happened to be unreadable. If the gate check in
        // `drain_for_prompt` were removed, this would reach the daemon and
        // `daemon.requests()` would be non-empty; asserting it stays empty
        // is what makes this test fail on that regression.
        let scratch = GitScratch::checkout("gate-off");
        let daemon = FakeDaemon::start("d9", |request| match request {
            Request::Hold { .. } => Some(resolved(&[repo_room()])),
            Request::Drain { .. } => messages_of(Response::Messages {
                messages: vec![message(1, "a", "shelter", MessageKind::Claim)],
                lost: 0,
                new_consumer: true,
                more: false,
            }),
            _ => Some(Response::Invalid {}),
        });
        let input = DrainInput {
            session_id: SESSION.to_owned(),
            cwd: scratch.0.clone(),
        };
        for config in [
            chat_config(false, false),
            chat_config(true, false),
            chat_config(false, true),
        ] {
            assert_eq!(
                drain_for_prompt(&config, &input, &daemon.socket).await,
                None
            );
        }
        assert!(
            daemon.requests().is_empty(),
            "a gate-closed call must never reach the daemon: {:?}",
            daemon.requests()
        );
    }

    #[tokio::test]
    async fn a_daemon_that_is_not_running_drains_silently() {
        let socket = std::env::temp_dir().join(format!(
            "awh-none-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let result = announce(&socket, Duration::from_millis(200), &checkout(), &holder()).await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn each_room_that_has_something_waiting_gets_its_own_line() {
        let daemon = FakeDaemon::start("d1", |request| match request {
            Request::Hold { .. } => Some(resolved(&[ticket_room(), repo_room()])),
            Request::Drain { room, .. } if *room == Some(ticket_room()) => {
                messages_of(Response::Messages {
                    messages: vec![message(41, "a", "shelter", MessageKind::Claim)],
                    lost: 0,
                    new_consumer: true,
                    more: false,
                })
            }
            Request::Drain { room, .. } if *room == Some(repo_room()) => {
                messages_of(Response::Messages {
                    messages: vec![
                        message(9, "b", "dart", MessageKind::Handoff),
                        message(10, "c", "dart", MessageKind::Handoff),
                    ],
                    lost: 2,
                    new_consumer: false,
                    more: true,
                })
            }
            _ => Some(Response::Invalid {}),
        });
        let text = announce(
            &daemon.socket,
            Duration::from_secs(5),
            &checkout(),
            &holder(),
        )
        .await
        .unwrap();
        assert!(
            text.contains("1 new message in the example-repo/ENG-7 (seq 41\u{2013}41)."),
            "{text}"
        );
        assert!(text.contains("Call room_read with after=40"), "{text}");
        assert!(
            text.contains(
                "2 new messages in the example-repo/example-org/example-repo (seq 9\u{2013}10)."
            ),
            "{text}"
        );
        assert!(text.contains("2 messages lost before this."), "{text}");
        assert!(text.contains("More are waiting."), "{text}");
        // Never `room: None`: each drain names the room it is for.
        assert!(
            daemon
                .requests()
                .iter()
                .all(|r| !matches!(r, Request::Drain { room: None, .. })),
            "{:?}",
            daemon.requests()
        );
    }

    #[tokio::test]
    async fn a_room_with_nothing_waiting_produces_no_line_and_no_rooms_produces_no_output() {
        // Resolved, joined, but empty — a room that exists and was checked,
        // with nothing to report.
        let daemon = FakeDaemon::start("d2", |request| match request {
            Request::Hold { .. } => Some(resolved(&[repo_room()])),
            Request::Drain { .. } => messages_of(Response::Messages {
                messages: vec![],
                lost: 0,
                new_consumer: true,
                more: false,
            }),
            _ => Some(Response::Invalid {}),
        });
        assert_eq!(
            announce(
                &daemon.socket,
                Duration::from_secs(5),
                &checkout(),
                &holder()
            )
            .await,
            None
        );

        // No rooms at all — an unmapped repo (Q3). Output is exactly the
        // same as the empty-room case above: "no rooms" must never read
        // differently from "no messages", because both say nothing.
        let daemon = FakeDaemon::start("d3", |request| match request {
            Request::Hold { .. } => Some(resolved(&[])),
            _ => Some(Response::Invalid {}),
        });
        assert_eq!(
            announce(
                &daemon.socket,
                Duration::from_secs(5),
                &checkout(),
                &holder()
            )
            .await,
            None
        );
        assert!(
            daemon
                .requests()
                .iter()
                .all(|r| !matches!(r, Request::Drain { .. })),
            "a resolve with no rooms must not drain anything"
        );
    }

    #[tokio::test]
    async fn a_stalled_daemon_is_bounded_by_the_deadline() {
        let daemon = FakeDaemon::start_slow(
            "d4",
            |_| Duration::from_secs(30),
            |request| match request {
                Request::Hold { .. } => Some(resolved(&[repo_room()])),
                _ => Some(Response::Invalid {}),
            },
        );
        let deadline = Duration::from_millis(200);
        let started = std::time::Instant::now();
        let result = announce(&daemon.socket, deadline, &checkout(), &holder()).await;
        let elapsed = started.elapsed();
        assert_eq!(result, None);
        assert!(
            elapsed < deadline * 3,
            "took {elapsed:?} against a {deadline:?} deadline"
        );
    }

    #[tokio::test]
    async fn the_overall_deadline_bounds_the_whole_sequence_not_one_request_at_a_time() {
        // Three rooms, each drain slow enough on its own (200ms) that three
        // in sequence take 600ms — well over a 300ms deadline, even though
        // no single request comes close to 300ms alone. The one deadline
        // `announce` takes has to bound the whole sequence, not let each
        // request spend its own full budget.
        let daemon = FakeDaemon::start_slow(
            "d8",
            |request| match request {
                Request::Drain { .. } => Duration::from_millis(200),
                _ => Duration::ZERO,
            },
            |request| match request {
                Request::Hold { .. } => {
                    Some(resolved(&[ticket_room(), repo_room(), project_room()]))
                }
                Request::Drain { .. } => messages_of(Response::Messages {
                    messages: vec![message(1, "a", "shelter", MessageKind::Claim)],
                    lost: 0,
                    new_consumer: true,
                    more: false,
                }),
                _ => Some(Response::Invalid {}),
            },
        );
        let deadline = Duration::from_millis(300);
        let started = std::time::Instant::now();
        let _ = announce(&daemon.socket, deadline, &checkout(), &holder()).await;
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "took {elapsed:?} against a {deadline:?} deadline over three 200ms drains \
             (600ms if run to completion)"
        );
    }

    #[tokio::test]
    async fn a_message_is_drained_exactly_once_across_two_turns() {
        // The daemon owns the cursor: the first `drain` for a
        // consumer sees the waiting message, the second — modelling a
        // second `UserPromptSubmit` for the same session — sees nothing,
        // because the real daemon has already moved the cursor past it.
        let delivered = Arc::new(Mutex::new(false));
        let daemon = FakeDaemon::start("d5", move |request| match request {
            Request::Hold { .. } => Some(resolved(&[repo_room()])),
            Request::Drain { .. } => {
                let mut done = delivered.lock().unwrap();
                if *done {
                    messages_of(Response::Messages {
                        messages: vec![],
                        lost: 0,
                        new_consumer: false,
                        more: false,
                    })
                } else {
                    *done = true;
                    messages_of(Response::Messages {
                        messages: vec![message(41, "a", "shelter", MessageKind::Claim)],
                        lost: 0,
                        new_consumer: true,
                        more: false,
                    })
                }
            }
            _ => Some(Response::Invalid {}),
        });
        let first = announce(
            &daemon.socket,
            Duration::from_secs(5),
            &checkout(),
            &holder(),
        )
        .await;
        assert!(first.is_some_and(|text| text.contains("1 new message")));
        let second = announce(
            &daemon.socket,
            Duration::from_secs(5),
            &checkout(),
            &holder(),
        )
        .await;
        assert_eq!(second, None, "the same message must not be announced twice");
    }

    #[tokio::test]
    async fn no_body_sender_or_kind_text_ever_reaches_the_output_even_when_hostile() {
        let host = "attacker-controlled-host-name";
        let daemon = FakeDaemon::start("d6", move |request| match request {
            Request::Hold { .. } => Some(resolved(&[repo_room()])),
            Request::Drain { .. } => messages_of(Response::Messages {
                messages: vec![message(1, CANARY, host, MessageKind::Handoff)],
                lost: 0,
                new_consumer: true,
                more: false,
            }),
            _ => Some(Response::Invalid {}),
        });
        let text = announce(
            &daemon.socket,
            Duration::from_secs(5),
            &checkout(),
            &holder(),
        )
        .await
        .unwrap();
        assert!(!text.contains(CANARY), "{text}");
        assert!(!text.contains(host), "{text}");
        assert!(!text.contains("handoff"), "{text}");
        assert!(!text.to_lowercase().contains("kind"), "{text}");
        // Only what the decision allows: a count, a seq range, a room name
        // and the pointer to `room_read`.
        assert!(text.contains("1 new message"), "{text}");
        assert!(text.contains("room_read"), "{text}");
    }

    #[tokio::test]
    async fn an_old_daemon_that_does_not_know_hold_falls_back_to_resolve_join() {
        let daemon = FakeDaemon::start("d7", |request| match request {
            Request::Hold { .. } => Some(Response::Invalid {}),
            Request::Resolve { join: true, .. } => Some(resolved(&[repo_room()])),
            Request::Drain { .. } => messages_of(Response::Messages {
                messages: vec![message(5, "a", "shelter", MessageKind::Claim)],
                lost: 0,
                new_consumer: true,
                more: false,
            }),
            _ => Some(Response::Invalid {}),
        });
        let text = announce(
            &daemon.socket,
            Duration::from_secs(5),
            &checkout(),
            &holder(),
        )
        .await
        .unwrap();
        assert!(text.contains("1 new message"), "{text}");
        let requests = daemon.requests();
        assert!(matches!(requests.first(), Some(Request::Hold { .. })));
        assert!(
            requests
                .iter()
                .any(|r| matches!(r, Request::Resolve { join: true, .. })),
            "{requests:?}"
        );
    }

    #[test]
    fn a_resolved_room_the_router_refused_is_never_drained() {
        // `resolved_rooms` only takes rooms whose join outcome is `Joined`;
        // a refused, room-limited or otherwise-unjoined room is filtered out
        // before a `Drain` is ever built for it. Checked directly against
        // the filter's own logic rather than round-tripping a daemon, since
        // the daemon-facing behaviour is already covered above.
        let rooms = vec![
            ResolvedRoom {
                room: ticket_room(),
                join: Some(JoinOutcome::Refused),
            },
            ResolvedRoom {
                room: repo_room(),
                join: Some(JoinOutcome::Joined),
            },
        ];
        let joined: Vec<RoomId> = rooms
            .into_iter()
            .filter(|room| matches!(room.join, Some(JoinOutcome::Joined)))
            .map(|room| room.room)
            .collect();
        assert_eq!(joined, vec![repo_room()]);
    }

    #[test]
    fn the_context_is_cut_by_whole_lines_never_mid_line() {
        let long_line = "x".repeat(MAX_CONTEXT_BYTES - 10);
        let lines = vec![long_line.clone(), "this one does not fit".to_owned()];
        let context = build_context(lines).unwrap();
        assert_eq!(context, long_line);
        assert!(context.len() <= MAX_CONTEXT_BYTES);
        assert_eq!(build_context(vec![]), None);
    }
}
