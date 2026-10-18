//! Where an agent is *working*, as opposed to where it was launched.
//!
//! A job or session file records the directory the agent started in. For anyone
//! whose repos sit side by side under one workspace directory, that is the
//! parent of every repo and a checkout of none — so the resolver finds no git
//! repo, the gate answers `NoRepo`, and `who` reports zero agents on a machine
//! that is busy. It reads exactly like a machine with nothing running, which is
//! the failure mode this tool exists to avoid.
//!
//! The transcript records a cwd on every message and it moves as the agent
//! works. The most recent one that is a checkout is the honest answer to "what
//! repo is this agent on".

use std::path::{Path, PathBuf};

use super::{fs::MAX_DIR_ENTRIES, fs::is_path_segment, roots::Root};

/// How much of the tail to read. A cwd appears on essentially every message, so
/// this reaches back far enough to find one in any transcript; the cap is here
/// because a transcript is vendor-owned and unbounded, and this runs on every
/// query with no cache (rule 44).
const TAIL_BYTES: u64 = 128 * 1024;

/// Find a session's transcript.
///
/// Claude Code files it under the encoded *current* working directory and moves
/// it when the agent changes directory, so the launch directory cannot be used
/// to compute the path — the file is simply not there any more. Scanning for it
/// by session id is what survives the agent moving, which is the whole case
/// this module exists for.
pub fn find(root: &Root, session_id: &str) -> Option<PathBuf> {
    if !is_path_segment(session_id) {
        return None;
    }
    let name = format!("{session_id}.jsonl");
    let projects = root.path().join("projects");
    let entries = std::fs::read_dir(projects).ok()?;

    let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
    for entry in entries.flatten().take(MAX_DIR_ENTRIES) {
        let path = entry.path().join(&name);
        let Ok(meta) = std::fs::metadata(&path) else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        let Ok(modified) = meta.modified() else {
            continue;
        };
        // There should be one, but a stale copy can be left behind by a move,
        // so prefer the most recently written.
        if best.as_ref().is_none_or(|(seen, _)| modified > *seen) {
            best = Some((modified, path));
        }
    }
    best.map(|(_, path)| path)
}

/// A directory is a checkout if it has a `.git`. A worktree's is a *file*, not
/// a directory, which is exactly the case that matters here — agents in this
/// repo run in `git worktree` checkouts.
fn is_checkout(dir: &Path) -> bool {
    dir.join(".git").exists()
}

/// Read at most the last `TAIL_BYTES` of a file. Returns `None` rather than a
/// partial answer when the file cannot be read at all.
fn tail(path: &Path) -> Option<Vec<u8>> {
    use std::io::{Read as _, Seek as _, SeekFrom};

    let mut file = std::fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    let from = len.saturating_sub(TAIL_BYTES);
    file.seek(SeekFrom::Start(from)).ok()?;

    let mut bytes = Vec::new();
    file.take(TAIL_BYTES).read_to_end(&mut bytes).ok()?;
    Some(bytes)
}

/// The most recent directory in this transcript that is a checkout.
///
/// Newest first, so an agent that has moved between repos reports the one it is
/// in now. Returns `None` when the transcript is unreadable or names no
/// checkout, and the caller keeps whatever it already had — a wrong directory
/// is worse than the launch directory, which at least fails visibly.
pub fn latest_checkout(path: &Path) -> Option<PathBuf> {
    let bytes = tail(path)?;

    // The first line is very likely a fragment, since the read started at a
    // byte offset rather than a line boundary. It simply fails to parse.
    for line in bytes.rsplit(|b| *b == b'\n') {
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(line) else {
            continue;
        };
        let Some(cwd) = value.get("cwd").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let dir = PathBuf::from(cwd);
        if dir.is_absolute() && is_checkout(&dir) {
            return Some(dir);
        }
    }
    None
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

    struct Fixture(PathBuf);

    impl Fixture {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("sealmesh-transcript-{name}"));
            std::fs::remove_dir_all(&dir).ok();
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir.canonicalize().unwrap())
        }

        /// A checkout, as git leaves one: `.git` is a directory for a clone.
        fn clone_at(&self, rel: &str) -> PathBuf {
            let dir = self.0.join(rel);
            std::fs::create_dir_all(dir.join(".git")).unwrap();
            dir.canonicalize().unwrap()
        }

        /// A `git worktree` checkout, where `.git` is a file.
        fn worktree_at(&self, rel: &str) -> PathBuf {
            let dir = self.0.join(rel);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(".git"), "gitdir: /elsewhere\n").unwrap();
            dir.canonicalize().unwrap()
        }

        fn plain_at(&self, rel: &str) -> PathBuf {
            let dir = self.0.join(rel);
            std::fs::create_dir_all(&dir).unwrap();
            dir.canonicalize().unwrap()
        }

        fn transcript(&self, cwds: &[&Path]) -> PathBuf {
            let path = self.0.join("session.jsonl");
            let body: String = cwds
                .iter()
                .map(|cwd| {
                    format!(
                        "{}\n",
                        serde_json::json!({ "type": "user", "cwd": cwd.to_str().unwrap() })
                    )
                })
                .collect();
            std::fs::write(&path, body).unwrap();
            path
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    #[test]
    fn the_launch_directory_loses_to_the_repo_the_agent_moved_into() {
        // The live case: a workspace holding several repos side by side. The
        // agent starts in the parent, which is a checkout of nothing, and works
        // in one of the repos under it.
        let f = Fixture::new("moved");
        let workspace = f.plain_at("workspace");
        let repo = f.clone_at("workspace/sealmesh");
        let path = f.transcript(&[&workspace, &repo, &workspace]);

        assert_eq!(latest_checkout(&path), Some(repo));
    }

    #[test]
    fn a_git_worktree_counts_even_though_its_dot_git_is_a_file() {
        let f = Fixture::new("worktree");
        let repo = f.clone_at("repo");
        let tree = f.worktree_at("repo/.claude/worktrees/feature");
        let path = f.transcript(&[&repo, &tree]);

        assert_eq!(latest_checkout(&path), Some(tree));
    }

    #[test]
    fn the_most_recent_repo_wins_when_an_agent_has_been_in_two() {
        let f = Fixture::new("two-repos");
        let first = f.clone_at("example-repo");
        let second = f.clone_at("sealmesh");
        let path = f.transcript(&[&first, &second]);

        assert_eq!(latest_checkout(&path), Some(second));
    }

    #[test]
    fn a_transcript_naming_no_checkout_answers_nothing_rather_than_guessing() {
        // The caller keeps the launch directory, which fails visibly in
        // `doctor`. Returning a plausible-looking wrong directory would
        // publish someone's work under the wrong repo.
        let f = Fixture::new("no-repo");
        let plain = f.plain_at("workspace");
        let path = f.transcript(&[&plain, &plain]);

        assert_eq!(latest_checkout(&path), None);
        assert_eq!(latest_checkout(&f.0.join("does-not-exist.jsonl")), None);
    }

    #[test]
    fn a_transcript_bigger_than_the_cap_still_finds_a_recent_directory() {
        let f = Fixture::new("huge");
        let repo = f.clone_at("repo");
        let path = f.0.join("session.jsonl");

        // Padding well past the cap, then the line that matters.
        let mut body = String::new();
        let filler = serde_json::json!({ "type": "assistant", "text": "x".repeat(4096) });
        while body.len() < (TAIL_BYTES as usize) * 2 {
            body.push_str(&format!("{filler}\n"));
        }
        body.push_str(&format!(
            "{}\n",
            serde_json::json!({ "type": "user", "cwd": repo.to_str().unwrap() })
        ));
        std::fs::write(&path, body).unwrap();

        assert_eq!(latest_checkout(&path), Some(repo));
    }

    #[test]
    fn a_line_split_by_the_tail_offset_is_skipped_not_fatal() {
        // The read starts mid-file, so the first line is usually a fragment.
        let f = Fixture::new("fragment");
        let repo = f.clone_at("repo");
        let path = f.0.join("session.jsonl");

        let mut body = "x".repeat(TAIL_BYTES as usize + 512);
        body.push('\n');
        body.push_str(&format!(
            "{}\n",
            serde_json::json!({ "type": "user", "cwd": repo.to_str().unwrap() })
        ));
        std::fs::write(&path, body).unwrap();

        assert_eq!(latest_checkout(&path), Some(repo));
    }
}
