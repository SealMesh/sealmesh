use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::wire::{JobId, RunState};

use super::{
    Candidate,
    fs::{MAX_DIR_ENTRIES, is_path_segment, read_capped},
    liveness::Liveness,
    roots::Root,
};

const MAX_SESSION_BYTES: u64 = 256 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawSession {
    #[serde(default)]
    pid: Option<u32>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    cwd: Option<PathBuf>,
    #[serde(default)]
    started_at: Option<i64>,
    #[serde(default)]
    updated_at: Option<i64>,
    #[serde(default)]
    status: Option<String>,
}

pub fn read_all(root: &Root, procs: &dyn Liveness) -> Vec<Candidate> {
    let Ok(entries) = std::fs::read_dir(root.sessions_dir()) else {
        return Vec::new();
    };
    let mut sessions: Vec<Candidate> = entries
        .flatten()
        .take(MAX_DIR_ENTRIES)
        .filter(|entry| entry.path().extension().is_some_and(|e| e == "json"))
        .filter_map(|entry| read_one(&entry.path(), root, procs))
        .collect();
    sessions.sort_by_key(|session| std::cmp::Reverse(session.updated_at));
    sessions
}

fn read_one(path: &Path, root: &Root, procs: &dyn Liveness) -> Option<Candidate> {
    let bytes = read_capped(path, MAX_SESSION_BYTES)?;
    let raw: RawSession = serde_json::from_slice(&bytes).ok()?;

    let pid = raw.pid?;
    let started_at_ms = raw.started_at?;

    if !procs.is(pid, started_at_ms) {
        return None;
    }

    let session_id = raw.session_id.filter(|s| is_path_segment(s))?;
    let id = JobId::try_from(short_id(&session_id)).ok()?;

    let work_dir = raw.cwd.filter(|p| p.is_absolute())?;

    let started_at = jiff::Timestamp::from_millisecond(started_at_ms).ok()?;
    let updated_at = transcript_mtime(root, &work_dir, &session_id)
        .or_else(|| {
            raw.updated_at
                .and_then(|ms| jiff::Timestamp::from_millisecond(ms).ok())
        })
        .unwrap_or(started_at);

    Some(Candidate {
        id,
        tool: crate::wire::ToolId::claude_code(),
        work_dir,
        session_id: Some(session_id),
        children: Vec::new(),
        run_state: run_state(raw.status.as_deref()),
        started_at,
        updated_at: updated_at.max(started_at),
        declared_branch: None,
        declared_pr: None,
        tasks_hint: None,
    })
}

fn short_id(session_id: &str) -> &str {
    match session_id.char_indices().nth(8) {
        Some((end, _)) => session_id.get(..end).unwrap_or(session_id),
        None => session_id,
    }
}

/// Where Claude Code files a session's transcript. Keyed on the directory the
/// session was *launched* in, which is why this takes `work_dir` rather than
/// deriving it from anything the agent did later.
pub fn transcript_path(root: &Root, launched_in: &Path, session_id: &str) -> Option<PathBuf> {
    let encoded = encode_cwd(launched_in.to_str()?);
    if !is_project_dir_name(&encoded) {
        return None;
    }
    Some(
        root.path()
            .join("projects")
            .join(encoded)
            .join(format!("{session_id}.jsonl")),
    )
}

fn transcript_mtime(root: &Root, work_dir: &Path, session_id: &str) -> Option<jiff::Timestamp> {
    let path = transcript_path(root, work_dir, session_id)?;

    let modified = std::fs::metadata(&path).ok()?.modified().ok()?;
    let since_epoch = modified.duration_since(std::time::UNIX_EPOCH).ok()?;
    jiff::Timestamp::from_millisecond(i64::try_from(since_epoch.as_millis()).ok()?).ok()
}

fn encode_cwd(cwd: &str) -> String {
    cwd.replace(['/', '.'], "-")
}

fn is_project_dir_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= NAME_MAX
        && !value.contains('/')
        && !value.contains('\\')
        && value != "."
        && value != ".."
}

/// `NAME_MAX` on Linux and macOS. A single path component cannot exceed it, so
/// a longer one names nothing that exists.
const NAME_MAX: usize = 255;

/// Map Claude Code's session status onto the closed wire enum.
fn run_state(status: Option<&str>) -> RunState {
    match status {
        Some("idle") => RunState::Idle,
        Some("waiting") => RunState::NeedsInput,
        _ => RunState::Working,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::collections::BTreeMap;

    use super::*;

    const CANARY: &str = "PASTED-SLACK-THREAD-NAMING-A-THIRD-PARTY";

    struct Procs(BTreeMap<u32, i64>);

    impl Liveness for Procs {
        fn started_at_secs(&self, pid: u32) -> Option<i64> {
            self.0.get(&pid).copied()
        }
    }

    struct Fixture(PathBuf);

    impl Fixture {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("sealmesh-sessions-{name}"));
            std::fs::remove_dir_all(&dir).ok();
            std::fs::create_dir_all(dir.join("sessions")).unwrap();
            Self(dir)
        }

        fn root(&self) -> Root {
            let roots = super::super::roots::discover(None, std::slice::from_ref(&self.0), None);
            roots.into_iter().next().expect("fixture is not a root")
        }

        fn session(&self, pid: u32, session_id: &str, cwd: &str, started_ms: i64, status: &str) {
            let json = serde_json::json!({
                "pid": pid,
                "sessionId": session_id,
                "cwd": cwd,
                "startedAt": started_ms,
                "updatedAt": started_ms + 5_000,
                "procStart": "Sun Aug 23 18:45:13 2026",
                "kind": "interactive",
                "entrypoint": "sdk-ts",
                "version": "2.1.241",
                "status": status,
                "statusUpdatedAt": started_ms + 5_000,
                "name": CANARY,
                "nameSource": "user",
                "waitingFor": CANARY,
                "messagingSocketPath": "/tmp/cc-socks/1.sock",
                "aFieldClaudeCodeAddsNextMonth": CANARY,
            });
            std::fs::write(
                self.0.join("sessions").join(format!("{pid}.json")),
                json.to_string(),
            )
            .unwrap();
        }

        fn transcript(&self, cwd: &str, session_id: &str) -> PathBuf {
            let dir = self.0.join("projects").join(encode_cwd(cwd));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join(format!("{session_id}.jsonl"));
            std::fs::write(&path, b"{}\n").unwrap();
            path
        }

        fn key_file(&self, pid: u32) {
            std::fs::write(
                self.0
                    .join("sessions")
                    .join(format!("{pid}.{}.key", "a".repeat(64))),
                "SECRET-PEER-TOKEN",
            )
            .unwrap();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    fn procs(entries: &[(u32, i64)]) -> Procs {
        Procs(entries.iter().copied().collect())
    }

    #[test]
    fn a_cwd_encodes_the_way_claude_code_files_it() {
        assert_eq!(
            encode_cwd("/Users/dart/work/checkout-service"),
            "-Users-dart-work-checkout-service"
        );
        assert_eq!(
            encode_cwd("/Users/dart/work/example-repo/.claude/worktrees/eng-612"),
            "-Users-dart-work-example-repo--claude-worktrees-eng-612"
        );
    }

    #[test]
    fn a_deep_worktree_path_is_not_refused_for_being_long() {
        let deep = "/Users/dart/Development/example-org/example-repo/.claude/worktrees/feature-eng-612-dispatch-records";
        let encoded = encode_cwd(deep);
        assert!(encoded.len() > 64, "fixture is not long enough: {encoded}");
        assert!(is_project_dir_name(&encoded), "refused: {encoded}");
    }

    #[test]
    fn a_project_dir_name_that_could_walk_out_is_refused() {
        assert!(!is_project_dir_name(""));
        assert!(!is_project_dir_name("."));
        assert!(!is_project_dir_name(".."));
        assert!(!is_project_dir_name("a/b"));
        assert!(!is_project_dir_name("a\\b"));
        assert!(!is_project_dir_name(&"a".repeat(NAME_MAX + 1)));
        assert!(is_project_dir_name("-Users-dart-my work-repo"));
        assert!(is_project_dir_name(&"a".repeat(NAME_MAX)));
    }

    #[test]
    fn updated_at_comes_from_the_transcript_of_a_worktree_session() {
        let f = Fixture::new("transcript");
        let cwd = "/Users/dart/work/example-repo/.claude/worktrees/eng-612-dispatch";
        let session_id = "d4d4d4d4-d94b-4d7e-9f02-5b09827a67f9";
        f.session(64653, session_id, cwd, 1_787_510_714_656, "busy");
        let transcript = f.transcript(cwd, session_id);

        let mtime = transcript_mtime(&f.root(), Path::new(cwd), session_id)
            .expect("the transcript was not found");

        let on_disk = std::fs::metadata(&transcript)
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        assert_eq!(
            mtime.as_millisecond(),
            i64::try_from(on_disk.as_millis()).unwrap()
        );

        let candidates = read_all(&f.root(), &procs(&[(64653, 1_787_510_713)]));
        let candidate = candidates.first().expect("no candidate");
        assert_eq!(candidate.updated_at, mtime);
    }

    #[test]
    fn a_live_session_becomes_a_candidate() {
        let f = Fixture::new("live");
        f.session(
            64653,
            "a04e3d0f-d94b-4d7e-9f02-5b09827a67f9",
            "/Users/work/Development",
            1_787_510_714_656,
            "busy",
        );

        let found = read_all(&f.root(), &procs(&[(64653, 1_787_510_713)]));
        assert_eq!(found.len(), 1, "{found:#?}");
        let session = found.first().unwrap();
        assert_eq!(session.id.as_str(), "a04e3d0f");
        assert_eq!(session.work_dir, PathBuf::from("/Users/work/Development"));
        assert_eq!(session.run_state, RunState::Working);
    }

    #[test]
    fn a_session_whose_process_is_gone_is_not_a_candidate() {
        let f = Fixture::new("dead");
        f.session(
            64653,
            "a04e3d0f-d94b-4d7e-9f02-5b09827a67f9",
            "/Users/work/Development",
            1_787_510_714_656,
            "busy",
        );

        assert!(read_all(&f.root(), &procs(&[])).is_empty());
        assert!(
            read_all(&f.root(), &procs(&[(64653, 1_787_510_713 + 3600)])).is_empty(),
            "a reused pid was accepted"
        );
    }

    #[test]
    fn no_canary_and_no_credential_reaches_the_candidate() {
        let f = Fixture::new("canary");
        f.session(
            64653,
            "a04e3d0f-d94b-4d7e-9f02-5b09827a67f9",
            "/Users/work/Development",
            1_787_510_714_656,
            "waiting",
        );
        f.key_file(64653);

        let found = read_all(&f.root(), &procs(&[(64653, 1_787_510_713)]));
        assert_eq!(found.len(), 1, "the key file was parsed as a session");
        let rendered = format!("{found:?}");
        assert!(!rendered.contains(CANARY), "{rendered}");
        assert!(!rendered.contains("SECRET-PEER-TOKEN"), "{rendered}");
    }

    #[test]
    fn the_status_words_map_to_the_closed_enum() {
        assert_eq!(run_state(Some("idle")), RunState::Idle);
        assert_eq!(run_state(Some("waiting")), RunState::NeedsInput);
        assert_eq!(run_state(Some("busy")), RunState::Working);
        assert_eq!(run_state(Some("shell")), RunState::Working);
        assert_eq!(run_state(None), RunState::Working);
        assert_eq!(run_state(Some("quiesced")), RunState::Working);
    }

    #[test]
    fn the_short_id_is_the_first_eight_characters() {
        assert_eq!(short_id("a04e3d0f-d94b-4d7e-9f02-5b09827a67f9"), "a04e3d0f");
        assert_eq!(short_id("abc"), "abc");
        assert_eq!(short_id(""), "");
    }

    #[test]
    fn a_session_id_that_could_walk_out_of_the_root_is_refused() {
        let f = Fixture::new("hostile");
        f.session(
            64653,
            "../../../../etc",
            "/Users/work/Development",
            1_787_510_714_656,
            "busy",
        );

        assert!(read_all(&f.root(), &procs(&[(64653, 1_787_510_713)])).is_empty());
    }

    #[test]
    fn a_relative_working_directory_is_refused() {
        let f = Fixture::new("relative");
        f.session(
            64653,
            "a04e3d0f-d94b-4d7e-9f02-5b09827a67f9",
            "../../etc",
            1_787_510_714_656,
            "busy",
        );

        assert!(read_all(&f.root(), &procs(&[(64653, 1_787_510_713)])).is_empty());
    }

    #[test]
    fn the_cap_is_far_above_a_real_session_file() {
        const { assert!(MAX_SESSION_BYTES >= 64 * 1024) };
    }
}
