//! Which Claude Code session mounted this server, for `room_post`'s drain key.
//!
//! The daemon's inbox hands each message once per *consumer key*. A post that
//! names its poster's own key is not drained back to that poster, so an
//! agent's own messages are not fed back into its context. The key the hook
//! drains a session with is the Claude Code session id, so that is the key
//! `room_post` passes when it can find it.
//!
//! **The key is never a tool input.** Any process running as this user can
//! name any key, so a tool that took one would let one agent consume another
//! agent's messages (Q5). `tools::check` refuses a `consumer` field in any
//! tool's arguments, and this module is the only place a key comes from.
//!
//! # How it is found, and what it may not do
//!
//! Claude Code spawns an stdio MCP server as its own child, and writes
//! `<config root>/sessions/<its pid>.json` holding its `sessionId`. So the key
//! is in the file named by this process's parent pid, under one of the config
//! roots: `$CLAUDE_CONFIG_DIR`, `$SEALMESH_CLAUDE_CONFIG_DIR`, then every
//! `~/.claude*` directory.
//!
//! This is a neutral reader, not the collector. The MCP crate may not call
//! `sealmesh::collect` (whose reads land in no audit ledger), and does not
//! need to: it reads one file whose name it already knows, and never lists a
//! sessions directory.
//!
//! Every read is bounded (rule 44): at most [`MAX_HOME_ENTRIES`] entries of the
//! home directory are looked at, at most [`MAX_ROOTS`] roots are tried, a
//! session file is opened non-blocking and must be a regular file on the
//! handle — a fifo would block forever — and at most
//! [`MAX_SESSION_FILE_BYTES`] of it is read. `room_post` also runs the whole
//! lookup on a blocking thread under a deadline, after its cooldown and body
//! checks, so a slow filesystem cannot stall the server either. The file is Claude Code's
//! and it adds fields without telling us, so unknown fields are discarded
//! (rule 43); what is kept goes through [`ConsumerKey::new`] (rules 9, 45).
//!
//! # When there is no key
//!
//! Anything unexpected is `None`, and `room_post` then posts with no key: the
//! post is the same, and only the echo suppression is lost, so the poster's
//! own message may come back to it once through the hook. That covers a
//! parent that is not Claude Code (Codex, a terminal), a file whose `pid` is
//! not the parent's, and two roots holding different sessions for the same
//! pid, which cannot be told apart and so names neither.
//!
//! A stale file left by an earlier Claude Code process that had the same pid
//! can still be read when the parent is not Claude Code. The cost is only
//! that the post is not drained to that dead session's key, which nothing
//! reads.

use std::{
    io::Read as _,
    os::unix::fs::OpenOptionsExt as _,
    path::{Path, PathBuf},
};

use sealmesh::room::local::ConsumerKey;
use serde::Deserialize;

/// The most bytes of a session file read. A real one is a few hundred bytes.
pub const MAX_SESSION_FILE_BYTES: u64 = 64 * 1024;

/// The most entries of the home directory looked at for `.claude*` roots.
pub const MAX_HOME_ENTRIES: usize = 4096;

/// The most config roots tried.
pub const MAX_ROOTS: usize = 16;

/// The fields kept from a session file. Everything else is discarded unread.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionFile {
    pid: Option<u32>,
    session_id: Option<String>,
}

/// The drain key of the Claude Code session that spawned this process, if
/// there is one.
pub fn consumer_key() -> Option<ConsumerKey> {
    let explicit: Vec<PathBuf> = ["CLAUDE_CONFIG_DIR", "SEALMESH_CLAUDE_CONFIG_DIR"]
        .into_iter()
        .filter_map(std::env::var_os)
        .map(PathBuf::from)
        .collect();
    let roots = roots(&explicit, dirs::home_dir().as_deref());
    key_for(&roots, std::os::unix::process::parent_id())
}

/// The config roots to try, explicit ones first, at most [`MAX_ROOTS`].
pub(crate) fn roots(explicit: &[PathBuf], home: Option<&Path>) -> Vec<PathBuf> {
    let mut roots: Vec<PathBuf> = explicit.to_vec();
    if let Some(home) = home
        && let Ok(entries) = std::fs::read_dir(home)
    {
        let mut found: Vec<PathBuf> = entries
            .take(MAX_HOME_ENTRIES)
            .flatten()
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(".claude"))
            })
            .map(|entry| entry.path())
            .collect();
        found.sort();
        roots.extend(found);
    }
    roots.dedup();
    roots.truncate(MAX_ROOTS);
    roots
}

/// The one session id every root agrees on for `pid`, or `None`.
pub(crate) fn key_for(roots: &[PathBuf], pid: u32) -> Option<ConsumerKey> {
    let mut found: Option<ConsumerKey> = None;
    for root in roots {
        let path = root.join("sessions").join(format!("{pid}.json"));
        let Some(key) = read_one(&path, pid) else {
            continue;
        };
        match &found {
            None => found = Some(key),
            Some(earlier) if *earlier == key => {}
            // Two roots name different sessions for one pid: at least one is
            // stale, and nothing here can say which.
            Some(_) => return None,
        }
    }
    found
}

fn read_one(path: &Path, pid: u32) -> Option<ConsumerKey> {
    // Opened non-blocking and checked on the handle, not the path: a check of
    // the path and then an open would race a swap to a fifo, and opening a
    // fifo for reading blocks until a writer appears. With `O_NONBLOCK` the
    // open returns at once whatever the file is, and `fstat` on the handle
    // says what was actually opened. Following a symlink is fine — its
    // target is this user's file either way — but it must be a regular file.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)
        .ok()?;
    if !file.metadata().ok()?.is_file() {
        return None;
    }
    let mut bytes = Vec::new();
    file.take(MAX_SESSION_FILE_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .ok()?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_SESSION_FILE_BYTES {
        return None;
    }
    let session: SessionFile = serde_json::from_slice(&bytes).ok()?;
    if session.pid? != pid {
        return None;
    }
    ConsumerKey::from_session_id(&session.session_id?)
}

/// How long `room_post` waits for [`consumer_key`] before posting without one.
pub const LOOKUP_DEADLINE: std::time::Duration = std::time::Duration::from_secs(2);

/// `lookup` on a blocking thread, or `None` once [`LOOKUP_DEADLINE`] (or
/// `deadline`, under test) passes. A lookup that never returns costs one
/// blocking-pool thread, never the server's one runtime thread.
pub async fn off_runtime<F>(lookup: F, deadline: std::time::Duration) -> Option<ConsumerKey>
where
    F: FnOnce() -> Option<ConsumerKey> + Send + 'static,
{
    match tokio::time::timeout(deadline, tokio::task::spawn_blocking(lookup)).await {
        Ok(Ok(key)) => key,
        Ok(Err(_)) | Err(_) => None,
    }
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

    struct Scratch(PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    impl Scratch {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir()
                .join(format!("sealmesh-mcp-session-{tag}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn root(&self, name: &str) -> PathBuf {
            let root = self.0.join(name);
            std::fs::create_dir_all(root.join("sessions")).unwrap();
            root
        }

        fn session(&self, root: &Path, pid: u32, contents: &str) {
            std::fs::write(root.join("sessions").join(format!("{pid}.json")), contents).unwrap();
        }
    }

    const SESSION: &str = "0b3e5c0a-6f7e-4a21-9d1c-5b7c1f0e2a11";

    fn file(pid: u32, id: &str) -> String {
        serde_json::json!({
            "pid": pid,
            "sessionId": id,
            "cwd": "/Users/dart/work",
            "startedAt": 1_700_000_000_000_i64,
            "prompt": "a pasted private thread",
        })
        .to_string()
    }

    #[test]
    fn the_parent_session_file_gives_its_session_id() {
        let scratch = Scratch::new("found");
        let root = scratch.root(".claude");
        scratch.session(&root, 4242, &file(4242, SESSION));
        assert_eq!(
            key_for(&[root], 4242),
            Some(ConsumerKey::new(SESSION).unwrap())
        );
    }

    #[test]
    fn a_file_for_another_pid_or_naming_another_pid_gives_nothing() {
        let scratch = Scratch::new("pid");
        let root = scratch.root(".claude");
        scratch.session(&root, 4242, &file(4243, SESSION));
        assert_eq!(key_for(std::slice::from_ref(&root), 4242), None);
        assert_eq!(key_for(&[root], 1), None);
    }

    #[test]
    fn a_session_id_that_is_not_a_valid_key_gives_nothing() {
        let scratch = Scratch::new("invalid");
        let root = scratch.root(".claude");
        for hostile in ["../../etc/passwd", "a b", "", &"k".repeat(200)] {
            scratch.session(&root, 4242, &file(4242, hostile));
            assert_eq!(
                key_for(std::slice::from_ref(&root), 4242),
                None,
                "{hostile:?}"
            );
        }
        scratch.session(&root, 4242, "not json");
        assert_eq!(key_for(std::slice::from_ref(&root), 4242), None);
        scratch.session(&root, 4242, r#"{"pid":4242}"#);
        assert_eq!(key_for(&[root], 4242), None);
    }

    #[test]
    fn an_oversized_file_is_refused_rather_than_read() {
        let scratch = Scratch::new("big");
        let root = scratch.root(".claude");
        let padding = " ".repeat(usize::try_from(MAX_SESSION_FILE_BYTES).unwrap());
        scratch.session(&root, 4242, &format!("{}{padding}", file(4242, SESSION)));
        assert_eq!(key_for(&[root], 4242), None);
    }

    #[test]
    fn a_fifo_is_refused_without_opening_it() {
        let scratch = Scratch::new("fifo");
        let root = scratch.root(".claude");
        let path = root.join("sessions").join("4242.json");
        let made = std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap();
        assert!(made.success());
        // Opening a fifo for reading blocks until a writer appears. It is
        // read on another thread so a regression fails here, not hangs.
        let (done, answer) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = done.send(key_for(&[root], 4242));
        });
        assert_eq!(
            answer
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("reading a fifo blocked"),
            None
        );
    }

    #[test]
    fn two_roots_that_disagree_name_neither_and_two_that_agree_name_it() {
        let scratch = Scratch::new("two");
        let a = scratch.root(".claude");
        let b = scratch.root(".claude-work");
        scratch.session(&a, 4242, &file(4242, SESSION));
        scratch.session(&b, 4242, &file(4242, "another-session"));
        assert_eq!(key_for(&[a.clone(), b.clone()], 4242), None);
        scratch.session(&b, 4242, &file(4242, SESSION));
        assert_eq!(
            key_for(&[a, b], 4242),
            Some(ConsumerKey::new(SESSION).unwrap())
        );
    }

    #[test]
    fn roots_are_explicit_ones_then_every_claude_directory_in_home() {
        let scratch = Scratch::new("roots");
        scratch.root(".claude");
        scratch.root(".claude-work");
        std::fs::create_dir_all(scratch.0.join("Documents")).unwrap();
        let explicit = scratch.0.join("elsewhere");
        let roots = roots(std::slice::from_ref(&explicit), Some(&scratch.0));
        assert_eq!(
            roots,
            vec![
                explicit,
                scratch.0.join(".claude"),
                scratch.0.join(".claude-work")
            ]
        );
    }

    #[test]
    fn the_number_of_roots_tried_is_capped() {
        let scratch = Scratch::new("cap");
        for n in 0..(MAX_ROOTS + 5) {
            std::fs::create_dir_all(scratch.0.join(format!(".claude-{n:02}"))).unwrap();
        }
        assert_eq!(roots(&[], Some(&scratch.0)).len(), MAX_ROOTS);
    }
}
