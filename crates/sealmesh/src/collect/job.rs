use std::path::{Path, PathBuf};

use jiff::Timestamp;
use serde::Deserialize;

use crate::wire::{JobId, RunState};

use super::{
    Candidate,
    fs::{MAX_DIR_ENTRIES, is_path_segment, read_capped},
    roots::Root,
};

const MAX_STATE_BYTES: u64 = 1024 * 1024;

const MAX_TASK_BYTES: u64 = 256 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawJob {
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    tempo: Option<String>,
    #[serde(default)]
    cwd: Option<PathBuf>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    children: Option<Vec<RawChild>>,
    #[serde(default)]
    created_at: Option<Timestamp>,
    #[serde(default)]
    updated_at: Option<Timestamp>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RawChild {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub href: Option<String>,
}

pub fn read_all(root: &Root) -> Vec<Candidate> {
    let Ok(entries) = std::fs::read_dir(root.jobs_dir()) else {
        return Vec::new();
    };
    let mut jobs: Vec<Candidate> = entries
        .flatten()
        .take(MAX_DIR_ENTRIES)
        .filter_map(|entry| read_one(&entry.path()))
        .collect();
    jobs.sort_by_key(|job| std::cmp::Reverse(job.updated_at));
    jobs
}

fn read_one(job_dir: &Path) -> Option<Candidate> {
    let id = JobId::try_from(job_dir.file_name()?.to_str()?).ok()?;
    let bytes = read_capped(&job_dir.join("state.json"), MAX_STATE_BYTES)?;
    let raw: RawJob = serde_json::from_slice(&bytes).ok()?;

    let started_at = raw.created_at?;
    let updated_at = raw.updated_at?;

    Some(Candidate {
        id,
        tool: crate::wire::ToolId::claude_code(),
        work_dir: raw.cwd.filter(|p| p.is_absolute())?,
        session_id: raw.session_id.filter(|s| is_path_segment(s)),
        children: raw.children.unwrap_or_default(),
        run_state: run_state(raw.state.as_deref(), raw.tempo.as_deref()),
        started_at,
        updated_at,
        declared_branch: None,
        declared_pr: None,
        tasks_hint: None,
    })
}

fn run_state(state: Option<&str>, tempo: Option<&str>) -> RunState {
    match (state, tempo) {
        (Some("done"), _) => RunState::Done,
        (Some("blocked"), _) | (_, Some("blocked")) => RunState::NeedsInput,
        (Some("working"), Some("idle")) => RunState::Idle,
        (Some("working"), _) => RunState::Working,
        _ => RunState::Unknown,
    }
}

pub fn task_progress(root: &Root, session_id: Option<&str>) -> Option<(u16, u16)> {
    #[derive(Deserialize)]
    struct RawTask {
        #[serde(default)]
        status: Option<String>,
    }

    let dir = root.tasks_dir(session_id?);
    let entries = std::fs::read_dir(dir).ok()?;

    let mut total: u16 = 0;
    let mut completed: u16 = 0;
    for entry in entries.flatten().take(MAX_DIR_ENTRIES) {
        if entry.path().extension().is_none_or(|e| e != "json") {
            continue;
        }
        let Some(bytes) = read_capped(&entry.path(), MAX_TASK_BYTES) else {
            continue;
        };
        let Ok(task) = serde_json::from_slice::<RawTask>(&bytes) else {
            continue;
        };
        total = total.saturating_add(1);
        if task.status.as_deref() == Some("completed") {
            completed = completed.saturating_add(1);
        }
    }

    (total > 0).then_some((completed, total))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn the_caps_are_far_above_a_real_job_file() {
        const { assert!(MAX_STATE_BYTES >= 256 * 1024) };
        const { assert!(MAX_TASK_BYTES >= 64 * 1024) };
    }

    #[test]
    fn blocked_means_needs_input_without_inferring_it() {
        assert_eq!(
            run_state(Some("working"), Some("blocked")),
            RunState::NeedsInput
        );
        for tempo in ["active", "idle", "blocked"] {
            assert_eq!(
                run_state(Some("blocked"), Some(tempo)),
                RunState::NeedsInput,
                "state=blocked tempo={tempo} did not read as needing input"
            );
        }
        assert_eq!(run_state(Some("blocked"), None), RunState::NeedsInput);

        assert_eq!(
            run_state(Some("working"), Some("active")),
            RunState::Working
        );
        assert_eq!(run_state(Some("working"), Some("idle")), RunState::Idle);
        assert_eq!(run_state(Some("done"), Some("idle")), RunState::Done);
        assert_eq!(run_state(Some("done"), Some("blocked")), RunState::Done);
        assert_eq!(run_state(Some("quiesced"), None), RunState::Unknown);
        assert_eq!(run_state(None, None), RunState::Unknown);
    }

    #[test]
    fn the_raw_prompt_fields_do_not_exist_on_the_parsed_job() {
        let json = br#"{
            "state": "working", "tempo": "active",
            "cwd": "/tmp", "sessionId": "abc",
            "createdAt": "2026-08-10T17:41:03.591Z",
            "updatedAt": "2026-08-10T19:53:28.829Z",
            "intent": "CANARY-PROMPT", "detail": "CANARY-DETAIL",
            "name": "CANARY-NAME", "nameSource": "auto",
            "providerEnv": {"SECRET": "CANARY-ENV"},
            "someFieldClaudeCodeAddsNextMonth": true
        }"#;
        let raw: RawJob = serde_json::from_slice(json).unwrap();
        assert_eq!(raw.state.as_deref(), Some("working"));
        assert_eq!(format!("{raw:?}").matches("CANARY").count(), 0);
    }
}
