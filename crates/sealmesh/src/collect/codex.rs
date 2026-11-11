//! Codex CLI sessions, read from the rollout files Codex already writes.
//!
//! Codex keeps one JSONL "rollout" per thread under
//! `$CODEX_HOME/sessions/YYYY/MM/DD/rollout-<local time>-<thread uuid>.jsonl`.
//! The first line is a `session_meta` record (the launch directory and when the
//! thread began); later lines are `turn_context` records (the directory each
//! turn ran in) and `event_msg` records, of which `task_started`,
//! `task_complete` and `turn_aborted` say whether a turn is open.
//!
//! Codex writes no pid and no session-end record a reader can rely on, so
//! liveness is the file's mtime: a rollout untouched for longer than
//! [`ACTIVE_WINDOW`] is not an agent. That is the same trade the generic
//! provider's `ttl` makes, written down rather than configured.
//!
//! Privacy is structural, as in the Claude readers. Each line is parsed into a
//! struct that has no field for message text, tool output, instructions or the
//! git remote, so none of it can reach a `Candidate` however Codex's format
//! grows. Only the first line and a bounded tail are read, never the middle.

use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead as _, BufReader, Read as _, Seek as _, SeekFrom},
    path::{Path, PathBuf},
};

use jiff::{SignedDuration, Timestamp};
use serde::{Deserialize, de::IgnoredAny};

use crate::wire::{JobId, RunState, ToolId};

use super::{
    Candidate,
    fs::{MAX_DIR_ENTRIES, is_path_segment},
};

/// A rollout written to within this long is a live session. Matches the
/// default TTL of a declared record and of a `[[provider]]` block, so every
/// mtime-based source in `who` ages out on the same clock.
pub const ACTIVE_WINDOW: SignedDuration = SignedDuration::from_secs(15 * 60);

/// Day directories older than this are not listed at all. Codex itself treats
/// a rollout a week old as cold enough to compress, and a session that started
/// that long ago and is still running is rare enough not to pay a directory walk
/// over every thread the machine ever had.
const LOOKBACK: SignedDuration = SignedDuration::from_hours(8 * 24);

/// At most this many recent rollouts are opened per scan.
const MAX_ROLLOUTS: usize = 32;

/// The `session_meta` line carries Codex's base instructions, which run to tens
/// of kilobytes. A first line longer than this is not a rollout we understand.
const MAX_HEAD_BYTES: u64 = 256 * 1024;

/// How much of the end of a rollout is read to find the open turn and its
/// directory. A turn whose output alone exceeds this reads as `Unknown`, which
/// is honest, rather than costing a read of the whole file.
const MAX_TAIL_BYTES: u64 = 128 * 1024;

/// Where Codex keeps its state: `CODEX_HOME` when set, as Codex itself
/// resolves it, otherwise `~/.codex`.
pub fn discover_home() -> Option<PathBuf> {
    let env = std::env::var_os("CODEX_HOME").map(PathBuf::from);
    home_from(env.as_deref(), dirs::home_dir().as_deref())
}

fn home_from(env: Option<&Path>, home: Option<&Path>) -> Option<PathBuf> {
    match env {
        Some(dir) if !dir.as_os_str().is_empty() => Some(dir.to_path_buf()),
        _ => home.map(|h| h.join(".codex")),
    }
}

/// A rollout file recent enough to be a live session, before it is opened.
#[derive(Debug, Clone)]
pub struct Rollout {
    pub path: PathBuf,
    pub thread_id: String,
    pub modified: Timestamp,
}

/// Rollouts modified within [`ACTIVE_WINDOW`] of `now`, newest first. Reads
/// directory entries and metadata only; no file is opened.
pub fn recent(home: &Path, now: Timestamp) -> Vec<Rollout> {
    let Some(oldest_day) = now
        .checked_sub(LOOKBACK)
        .ok()
        .map(|t| t.to_zoned(jiff::tz::TimeZone::UTC).date())
    else {
        return Vec::new();
    };
    let oldest = (oldest_day.year(), oldest_day.month(), oldest_day.day());

    let mut found = Vec::new();
    for (year, year_dir) in numbered(&home.join("sessions"), 4) {
        if year < i32::from(oldest.0) {
            continue;
        }
        for (month, month_dir) in numbered(&year_dir, 2) {
            if (year, month) < (i32::from(oldest.0), i32::from(oldest.1)) {
                continue;
            }
            for (day, day_dir) in numbered(&month_dir, 2) {
                if (year, month, day)
                    < (
                        i32::from(oldest.0),
                        i32::from(oldest.1),
                        i32::from(oldest.2),
                    )
                {
                    continue;
                }
                found.extend(rollouts_in(&day_dir, now));
            }
        }
    }
    found.sort_by_key(|r| std::cmp::Reverse(r.modified));
    found.truncate(MAX_ROLLOUTS);
    found
}

/// Subdirectories whose name is exactly `width` ASCII digits, with the number.
fn numbered(dir: &Path, width: usize) -> Vec<(i32, PathBuf)> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .take(MAX_DIR_ENTRIES)
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str()?;
            if name.len() != width || !name.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let path = entry.path();
            path.is_dir().then_some((name.parse().ok()?, path))
        })
        .collect()
}

fn rollouts_in(day_dir: &Path, now: Timestamp) -> Vec<Rollout> {
    let Ok(entries) = std::fs::read_dir(day_dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .take(MAX_DIR_ENTRIES)
        .filter_map(|entry| {
            let name = entry.file_name();
            let thread_id = thread_id(name.to_str()?)?.to_owned();
            let path = entry.path();
            let meta = std::fs::metadata(&path).ok()?;
            if !meta.is_file() {
                return None;
            }
            let modified = mtime(&meta)?;
            if now.duration_since(modified) > ACTIVE_WINDOW {
                return None;
            }
            Some(Rollout {
                path,
                thread_id,
                modified,
            })
        })
        .collect()
}

fn mtime(meta: &std::fs::Metadata) -> Option<Timestamp> {
    let since_epoch = meta
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?;
    Timestamp::from_millisecond(i64::try_from(since_epoch.as_millis()).ok()?).ok()
}

/// The thread uuid from a rollout's file name.
///
/// `rollout-2026-11-10T09-15-02-<uuid>.jsonl`, or `...-<uuid>_<rollout uuid>`
/// for a thread Codex reverted. The thread is the stable identity, so a revert
/// does not turn one agent into two. Compressed `.jsonl.zst` rollouts are
/// skipped: Codex only compresses files a week cold.
fn thread_id(file_name: &str) -> Option<&str> {
    let core = file_name.strip_prefix("rollout-")?.strip_suffix(".jsonl")?;
    let ids = core.get(20..)?;
    if core.get(19..20)? != "-" {
        return None;
    }
    let thread = ids.split_once('_').map_or(ids, |(thread, _)| thread);
    is_uuid(thread).then_some(thread)
}

fn is_uuid(value: &str) -> bool {
    value.len() == 36
        && value.char_indices().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_digit() || matches!(c, 'a'..='f'),
        })
}

/// The row id: the last eight hex digits of the thread uuid.
///
/// Not the first eight, which is what the Claude reader takes. Codex mints
/// UUIDv7s, whose leading digits are a timestamp, so two sessions started in
/// the same minute would share them. The tail is random.
fn short_id(thread_id: &str) -> Option<JobId> {
    let tail = thread_id.get(thread_id.len().checked_sub(8)?..)?;
    JobId::try_from(tail).ok()
}

/// One rollout line, with the payload typed per record kind.
#[derive(Deserialize)]
struct Line<P> {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    timestamp: Option<String>,
    payload: P,
}

#[derive(Debug, Deserialize)]
struct MetaPayload {
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default)]
    cwd: Option<PathBuf>,
    #[serde(default)]
    source: Option<RawSource>,
    #[serde(default)]
    parent_thread_id: Option<IgnoredAny>,
}

/// `source` is a bare word (`cli`, `vscode`, `exec`) or a one-key object
/// (`{"subagent": ...}`). Only an object's keys are kept; a word is discarded
/// unread, because no word names a background thread.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawSource {
    Tagged(BTreeMap<String, IgnoredAny>),
    Other(IgnoredAny),
}

impl RawSource {
    /// A thread Codex spawned for itself rather than one a person is driving.
    /// Shown as its own agent it would double-count the session that owns it.
    fn is_background(&self) -> bool {
        match self {
            Self::Tagged(map) => map.contains_key("subagent") || map.contains_key("internal"),
            Self::Other(_) => false,
        }
    }
}

#[derive(Deserialize)]
struct EventPayload {
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
struct TurnContextPayload {
    #[serde(default)]
    cwd: Option<PathBuf>,
}

#[derive(Deserialize)]
struct Kind {
    #[serde(rename = "type")]
    kind: String,
}

/// What the end of a rollout says: whether a turn is open, and where the most
/// recent turn ran.
#[derive(Debug, Default, PartialEq, Eq)]
struct Tail {
    run_state: Option<RunState>,
    cwd: Option<PathBuf>,
}

fn lifecycle(event: &str) -> Option<RunState> {
    match event {
        "task_started" | "turn_started" => Some(RunState::Working),
        "task_complete" | "turn_complete" | "turn_aborted" => Some(RunState::Idle),
        _ => None,
    }
}

/// Walk the tail backwards and stop at the first lifecycle event and the first
/// absolute turn directory. A line that does not parse is skipped, not fatal:
/// the last line of a file being appended to is routinely half-written.
fn read_tail(bytes: &[u8], starts_mid_line: bool) -> Tail {
    let mut lines: Vec<&[u8]> = bytes.split(|b| *b == b'\n').collect();
    if starts_mid_line && !lines.is_empty() {
        lines.remove(0);
    }

    let mut tail = Tail::default();
    for line in lines.into_iter().rev() {
        if tail.run_state.is_some() && tail.cwd.is_some() {
            break;
        }
        let Ok(Kind { kind }) = serde_json::from_slice::<Kind>(line) else {
            continue;
        };
        match kind.as_str() {
            "event_msg" if tail.run_state.is_none() => {
                if let Ok(event) = serde_json::from_slice::<Line<EventPayload>>(line) {
                    tail.run_state = lifecycle(&event.payload.kind);
                }
            }
            "turn_context" if tail.cwd.is_none() => {
                if let Ok(turn) = serde_json::from_slice::<Line<TurnContextPayload>>(line) {
                    tail.cwd = turn.payload.cwd.filter(|p| p.is_absolute());
                }
            }
            _ => {}
        }
    }
    tail
}

/// The first line, if it fits under the cap.
fn read_head(file: &mut File) -> Option<Vec<u8>> {
    let mut head = Vec::new();
    BufReader::new(file.take(MAX_HEAD_BYTES.saturating_add(1)))
        .read_until(b'\n', &mut head)
        .ok()?;
    if u64::try_from(head.len()).ok()? > MAX_HEAD_BYTES {
        return None;
    }
    Some(head)
}

fn read_last(file: &mut File, len: u64) -> Option<(Vec<u8>, bool)> {
    let start = len.saturating_sub(MAX_TAIL_BYTES);
    file.seek(SeekFrom::Start(start)).ok()?;
    let mut bytes = Vec::new();
    file.take(MAX_TAIL_BYTES).read_to_end(&mut bytes).ok()?;
    Some((bytes, start > 0))
}

/// One rollout as a candidate, or nothing if it is not a session a person is
/// running in a directory we can name.
pub fn read_one(rollout: &Rollout) -> Option<Candidate> {
    let mut file = File::open(&rollout.path).ok()?;
    let len = file.metadata().ok()?.len();

    let head = read_head(&mut file)?;
    let meta: Line<MetaPayload> = serde_json::from_slice(&head).ok()?;
    if meta.kind != "session_meta"
        || meta.payload.parent_thread_id.is_some()
        || meta
            .payload
            .source
            .as_ref()
            .is_some_and(RawSource::is_background)
    {
        return None;
    }
    let started_at: Timestamp = meta
        .payload
        .timestamp
        .as_deref()
        .or(meta.timestamp.as_deref())?
        .parse()
        .ok()?;

    let (bytes, starts_mid_line) = read_last(&mut file, len)?;
    let tail = read_tail(&bytes, starts_mid_line);

    let work_dir = tail
        .cwd
        .or_else(|| meta.payload.cwd.filter(|p| p.is_absolute()))?;
    let session_id = Some(rollout.thread_id.clone()).filter(|s| is_path_segment(s))?;

    Some(Candidate {
        id: short_id(&rollout.thread_id)?,
        tool: ToolId::codex(),
        work_dir,
        session_id: Some(session_id),
        children: Vec::new(),
        run_state: tail.run_state.unwrap_or(RunState::Unknown),
        started_at,
        updated_at: rollout.modified.max(started_at),
        declared_branch: None,
        declared_pr: None,
        tasks_hint: None,
    })
}

/// Every live Codex session under `home`.
pub fn read_all(home: &Path, now: Timestamp) -> Vec<Candidate> {
    recent(home, now).iter().filter_map(read_one).collect()
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use std::io::Write as _;

    use super::*;

    const CANARY: &str = "CANARY-PROMPT-TEXT-THAT-MUST-NOT-LEAVE";
    const THREAD: &str = "019a6f2e-1c3b-7d40-9e55-0a1b2c3d4e5f";
    const OTHER_THREAD: &str = "019a6f2e-1c3b-7d40-8a11-ffeeddccbbaa";

    struct Home(PathBuf);

    impl Home {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("sealmesh-codex-{name}"));
            std::fs::remove_dir_all(&dir).ok();
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn day(&self, now: Timestamp) -> PathBuf {
            let date = now.to_zoned(jiff::tz::TimeZone::UTC).date();
            let dir = self
                .0
                .join("sessions")
                .join(format!("{:04}", date.year()))
                .join(format!("{:02}", date.month()))
                .join(format!("{:02}", date.day()));
            std::fs::create_dir_all(&dir).unwrap();
            dir
        }

        fn rollout(&self, now: Timestamp, thread: &str, lines: &[String]) -> PathBuf {
            let path = self
                .day(now)
                .join(format!("rollout-2026-11-10T09-15-02-{thread}.jsonl"));
            let mut file = File::create(&path).unwrap();
            for line in lines {
                writeln!(file, "{line}").unwrap();
            }
            path
        }
    }

    impl Drop for Home {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    fn meta(cwd: &str, source: serde_json::Value) -> String {
        serde_json::json!({
            "timestamp": "2026-11-10T09:15:02.481Z",
            "type": "session_meta",
            "payload": {
                "id": THREAD,
                "session_id": THREAD,
                "timestamp": "2026-11-10T09:15:02.123Z",
                "cwd": cwd,
                "originator": "codex_cli_rs",
                "cli_version": "0.99.0",
                "source": source,
                "model_provider": "openai",
                "base_instructions": {"text": CANARY},
                "git": {
                    "commit_hash": "0123456789abcdef0123456789abcdef01234567",
                    "branch": "feature/fake",
                    "repository_url": "https://example.invalid/secret-org/secret-repo.git"
                }
            }
        })
        .to_string()
    }

    fn event(kind: &str) -> String {
        serde_json::json!({
            "timestamp": "2026-11-10T09:16:00.000Z",
            "type": "event_msg",
            "payload": {"type": kind, "turn_id": "turn-1", "last_agent_message": CANARY}
        })
        .to_string()
    }

    fn turn_context(cwd: &str) -> String {
        serde_json::json!({
            "timestamp": "2026-11-10T09:16:00.000Z",
            "type": "turn_context",
            "payload": {"turn_id": "turn-1", "cwd": cwd, "model": "fake-model", "user_instructions": CANARY}
        })
        .to_string()
    }

    fn message(text: &str) -> String {
        serde_json::json!({
            "timestamp": "2026-11-10T09:16:01.000Z",
            "type": "response_item",
            "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": text}]}
        })
        .to_string()
    }

    fn age(path: &Path, by: SignedDuration) {
        let when = Timestamp::now().checked_sub(by).unwrap();
        let when = std::time::UNIX_EPOCH
            + std::time::Duration::from_millis(u64::try_from(when.as_millisecond()).unwrap());
        File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(when)
            .unwrap();
    }

    #[test]
    fn a_live_session_with_an_open_turn_is_a_working_agent() {
        let home = Home::new("live");
        let now = Timestamp::now();
        home.rollout(
            now,
            THREAD,
            &[
                meta("/work/launched-here", serde_json::json!("cli")),
                turn_context("/work/example-repo"),
                message(CANARY),
                event("task_started"),
                message(CANARY),
            ],
        );

        let found = read_all(&home.0, now);
        assert_eq!(found.len(), 1, "{found:#?}");
        let agent = found.first().unwrap();
        assert_eq!(agent.tool, ToolId::codex());
        assert_eq!(agent.id.as_str(), "2c3d4e5f");
        assert_eq!(agent.run_state, RunState::Working);
        assert_eq!(agent.work_dir, PathBuf::from("/work/example-repo"));
        assert_eq!(agent.session_id.as_deref(), Some(THREAD));
        assert_eq!(
            agent.started_at,
            "2026-11-10T09:15:02.123Z".parse::<Timestamp>().unwrap()
        );
        assert!(agent.updated_at >= agent.started_at);
    }

    #[test]
    fn a_finished_turn_is_idle_and_an_aborted_one_too() {
        for last in ["task_complete", "turn_aborted"] {
            let home = Home::new(&format!("idle-{last}"));
            let now = Timestamp::now();
            home.rollout(
                now,
                THREAD,
                &[
                    meta("/work/example-repo", serde_json::json!("cli")),
                    event("task_started"),
                    event(last),
                ],
            );
            let found = read_all(&home.0, now);
            assert_eq!(found.len(), 1, "{last}");
            assert_eq!(found.first().unwrap().run_state, RunState::Idle, "{last}");
        }
    }

    #[test]
    fn without_a_turn_context_the_launch_directory_is_used() {
        let home = Home::new("launchdir");
        let now = Timestamp::now();
        home.rollout(
            now,
            THREAD,
            &[meta("/work/example-repo", serde_json::json!("vscode"))],
        );
        let found = read_all(&home.0, now);
        let agent = found.first().expect("no agent");
        assert_eq!(agent.work_dir, PathBuf::from("/work/example-repo"));
        assert_eq!(agent.run_state, RunState::Unknown);
    }

    #[test]
    fn a_session_nobody_has_written_to_lately_is_not_an_agent() {
        let home = Home::new("stale");
        let now = Timestamp::now();
        let path = home.rollout(
            now,
            THREAD,
            &[
                meta("/work/example-repo", serde_json::json!("cli")),
                event("task_started"),
            ],
        );
        age(&path, ACTIVE_WINDOW + SignedDuration::from_secs(60));

        assert!(read_all(&home.0, now).is_empty());
        assert!(recent(&home.0, now).is_empty());
    }

    #[test]
    fn a_day_directory_older_than_the_lookback_is_not_walked() {
        let home = Home::new("oldday");
        let now = Timestamp::now();
        let long_ago = now
            .checked_sub(LOOKBACK + SignedDuration::from_hours(48))
            .unwrap();
        // Fresh mtime, ancient directory: only the directory walk can drop it.
        home.rollout(
            long_ago,
            THREAD,
            &[meta("/work/example-repo", serde_json::json!("cli"))],
        );
        assert!(read_all(&home.0, now).is_empty());
    }

    #[test]
    fn a_malformed_line_is_skipped_and_the_rest_still_read() {
        let home = Home::new("malformed");
        let now = Timestamp::now();
        home.rollout(
            now,
            THREAD,
            &[
                meta("/work/launched-here", serde_json::json!("cli")),
                turn_context("/work/example-repo"),
                event("task_started"),
                "{\"type\": \"event_msg\", \"payload\": {\"type\": \"task_compl".to_owned(),
                "not json at all".to_owned(),
                "{\"type\": 7}".to_owned(),
            ],
        );
        let found = read_all(&home.0, now);
        let agent = found.first().expect("a bad line sank the whole file");
        assert_eq!(agent.run_state, RunState::Working);
        assert_eq!(agent.work_dir, PathBuf::from("/work/example-repo"));
    }

    #[test]
    fn a_malformed_first_line_drops_only_that_rollout() {
        let home = Home::new("badhead");
        let now = Timestamp::now();
        home.rollout(now, THREAD, &["{\"type\": \"session_meta\"".to_owned()]);
        home.rollout(
            now,
            OTHER_THREAD,
            &[meta("/work/example-repo", serde_json::json!("cli"))],
        );
        let found = read_all(&home.0, now);
        assert_eq!(found.len(), 1, "{found:#?}");
        assert_eq!(
            found.first().unwrap().session_id.as_deref(),
            Some(OTHER_THREAD)
        );
    }

    #[test]
    fn a_huge_rollout_is_read_only_at_its_ends() {
        let home = Home::new("huge");
        let now = Timestamp::now();
        let filler = message(&"x".repeat(4096));
        let mut lines = vec![
            meta("/work/launched-here", serde_json::json!("cli")),
            turn_context("/work/example-repo"),
            event("task_started"),
        ];
        let needed = usize::try_from(MAX_TAIL_BYTES).unwrap() / filler.len() + 4;
        lines.extend(std::iter::repeat_n(filler, needed));
        let path = home.rollout(now, THREAD, &lines);
        assert!(std::fs::metadata(&path).unwrap().len() > MAX_TAIL_BYTES);

        // The turn marker and directory sit in the unread middle, so the row
        // keeps the launch directory and does not guess a state.
        let found = read_all(&home.0, now);
        let agent = found.first().expect("a large rollout was dropped");
        assert_eq!(agent.run_state, RunState::Unknown);
        assert_eq!(agent.work_dir, PathBuf::from("/work/launched-here"));
    }

    #[test]
    fn a_first_line_over_the_cap_is_refused() {
        let home = Home::new("hugehead");
        let now = Timestamp::now();
        let bloated = serde_json::json!({
            "timestamp": "2026-11-10T09:15:02.481Z",
            "type": "session_meta",
            "payload": {
                "timestamp": "2026-11-10T09:15:02.123Z",
                "cwd": "/work/example-repo",
                "base_instructions": {"text": "y".repeat(usize::try_from(MAX_HEAD_BYTES).unwrap())}
            }
        })
        .to_string();
        home.rollout(now, THREAD, &[bloated]);
        assert!(read_all(&home.0, now).is_empty());
    }

    #[test]
    fn subagent_and_internal_threads_are_not_separate_agents() {
        for (name, source, parent) in [
            ("sub", serde_json::json!({"subagent": "review"}), false),
            (
                "internal",
                serde_json::json!({"internal": {"kind": "memory"}}),
                false,
            ),
            ("parent", serde_json::json!("cli"), true),
        ] {
            let home = Home::new(&format!("bg-{name}"));
            let now = Timestamp::now();
            let mut line: serde_json::Value =
                serde_json::from_str(&meta("/work/example-repo", source)).unwrap();
            if parent {
                line["payload"]["parent_thread_id"] = serde_json::json!(OTHER_THREAD);
            }
            home.rollout(now, THREAD, &[line.to_string()]);
            assert!(read_all(&home.0, now).is_empty(), "{name} became an agent");
        }
    }

    #[test]
    fn a_relative_directory_is_refused() {
        let home = Home::new("relative");
        let now = Timestamp::now();
        home.rollout(
            now,
            THREAD,
            &[
                meta("../../etc", serde_json::json!("cli")),
                turn_context("also/relative"),
            ],
        );
        assert!(read_all(&home.0, now).is_empty());
    }

    #[test]
    fn no_text_from_the_rollout_reaches_the_candidate() {
        let home = Home::new("canary");
        let now = Timestamp::now();
        home.rollout(
            now,
            THREAD,
            &[
                meta("/work/example-repo", serde_json::json!({"custom": CANARY})),
                turn_context("/work/example-repo"),
                message(CANARY),
                event("task_complete"),
            ],
        );
        let found = read_all(&home.0, now);
        assert_eq!(found.len(), 1);
        let rendered = format!("{found:?}");
        assert!(!rendered.contains(CANARY), "{rendered}");
        assert!(!rendered.contains("secret-org"), "{rendered}");
        assert!(!rendered.contains("feature/fake"), "{rendered}");
    }

    #[test]
    fn no_codex_home_yields_nothing() {
        let missing = std::env::temp_dir().join("sealmesh-codex-does-not-exist");
        std::fs::remove_dir_all(&missing).ok();
        assert!(read_all(&missing, Timestamp::now()).is_empty());

        let empty = Home::new("empty");
        assert!(read_all(&empty.0, Timestamp::now()).is_empty());
    }

    #[test]
    fn stray_files_in_the_tree_are_ignored() {
        let home = Home::new("stray");
        let now = Timestamp::now();
        let day = home.day(now);
        std::fs::write(
            day.join("notes.jsonl"),
            meta("/w", serde_json::json!("cli")),
        )
        .unwrap();
        std::fs::write(
            day.join(format!("rollout-2026-11-10T09-15-02-{THREAD}.jsonl.zst")),
            b"\x28\xb5\x2f\xfd",
        )
        .unwrap();
        std::fs::create_dir_all(
            day.join(format!("rollout-2026-11-10T09-15-02-{OTHER_THREAD}.jsonl")),
        )
        .unwrap();
        std::fs::create_dir_all(home.0.join("sessions").join("latest")).unwrap();
        assert!(read_all(&home.0, now).is_empty());
    }

    #[test]
    fn codex_home_honours_the_env_var_and_falls_back_to_dot_codex() {
        let home = Path::new("/home/fake");
        assert_eq!(
            home_from(Some(Path::new("/opt/codex-state")), Some(home)),
            Some(PathBuf::from("/opt/codex-state"))
        );
        assert_eq!(
            home_from(Some(Path::new("")), Some(home)),
            Some(PathBuf::from("/home/fake/.codex"))
        );
        assert_eq!(
            home_from(None, Some(home)),
            Some(PathBuf::from("/home/fake/.codex"))
        );
        assert_eq!(home_from(None, None), None);
    }

    #[test]
    fn a_file_name_yields_the_thread_and_nothing_else() {
        assert_eq!(
            thread_id(&format!("rollout-2026-11-10T09-15-02-{THREAD}.jsonl")),
            Some(THREAD)
        );
        assert_eq!(
            thread_id(&format!(
                "rollout-2026-11-10T09-15-02-{THREAD}_{OTHER_THREAD}.jsonl"
            )),
            Some(THREAD),
            "a reverted thread must keep its identity"
        );
        for hostile in [
            "rollout-2026-11-10T09-15-02-../../etc/passwd.jsonl".to_owned(),
            format!(
                "rollout-2026-11-10T09-15-02-{}.jsonl",
                THREAD.to_uppercase()
            ),
            format!("rollout-2026-11-10T09-15-02-{THREAD}.jsonl.zst"),
            format!("rollout-{THREAD}.jsonl"),
            "rollout-.jsonl".to_owned(),
            String::new(),
        ] {
            assert_eq!(thread_id(&hostile), None, "accepted {hostile:?}");
        }
    }

    #[test]
    fn the_short_id_is_the_random_tail_of_the_uuid() {
        assert_eq!(short_id(THREAD).unwrap().as_str(), "2c3d4e5f");
        assert_ne!(short_id(THREAD), short_id(OTHER_THREAD));
        assert_eq!(short_id("abc"), None);
    }

    #[test]
    fn at_most_the_cap_of_rollouts_is_opened() {
        let home = Home::new("many");
        let now = Timestamp::now();
        for i in 0..MAX_ROLLOUTS + 5 {
            let thread = format!("019a6f2e-1c3b-7d40-9e55-{i:012x}");
            home.rollout(
                now,
                &thread,
                &[meta("/work/example-repo", serde_json::json!("cli"))],
            );
        }
        assert_eq!(recent(&home.0, now).len(), MAX_ROLLOUTS);
    }

    #[test]
    fn the_caps_are_far_above_a_real_session_meta_line() {
        const { assert!(MAX_HEAD_BYTES >= 64 * 1024) };
        const { assert!(MAX_TAIL_BYTES >= 32 * 1024) };
    }
}
