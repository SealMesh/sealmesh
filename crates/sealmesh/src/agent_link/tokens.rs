//! Each configured agent's bearer token for `agents.sock`.
//!
//! An agent is named in config (`[[agent]] name = "grokbot"`), never by
//! anything it sends. The daemon proves the name with a token it issues: on
//! start, every configured agent gets `<name>.token` (32 random bytes as 64
//! hex characters, 0600, in a 0700 directory) if it has none, and a request
//! carrying that token is that agent. The file is how the operator hands the
//! token to the bot, and it is also the only copy: nothing else records it.
//!
//! - A token file for a name no `[[agent]]` block lists is deleted, so
//!   removing an agent from the config and restarting the daemon revokes it.
//! - A token file someone else could read or write, or that does not hold a
//!   well-formed token, is replaced with a fresh one: a token others could see
//!   proves nothing about who holds it.
//! - A symlink, directory or anything else that is not a regular file where a
//!   token belongs stops the daemon with a message rather than being followed
//!   or removed (rule 33).
//!
//! Lookup compares the presented token against every agent's in constant
//! time and never stops at the first match, so how long a refusal takes says
//! nothing about which agent's token it resembled.

use std::{
    io::ErrorKind,
    os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use rand::{TryRngCore as _, rngs::OsRng};
use sealmesh_proto::SecretToken;
use sealmesh_wire::HostId;
use subtle::ConstantTimeEq as _;

/// Bytes of randomness in a token; it is written as twice as many hex
/// characters.
pub const TOKEN_BYTES: usize = 32;

/// The extension a token file has. Nothing else in the directory is touched.
pub const EXTENSION: &str = "token";

/// The most directory entries one sync looks at when it clears revoked
/// tokens. The directory is this user's own and 0700.
const MAX_ENTRIES: usize = 1024;

/// What a sync did, for the daemon's startup lines and `doctor`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Synced {
    /// Agents that had no token and now have one.
    pub created: Vec<HostId>,
    /// Agents whose token file was shared, malformed or not this user's, and
    /// was replaced.
    pub replaced: Vec<HostId>,
    /// Token files for agents no longer configured, deleted.
    pub revoked: Vec<String>,
}

/// Every configured agent's token, held only by the daemon.
pub struct Tokens {
    entries: Vec<(HostId, SecretToken)>,
}

impl std::fmt::Debug for Tokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.entries.iter().map(|(name, _)| name))
            .finish()
    }
}

impl Tokens {
    /// Brings `dir` in line with `agents`: issues missing tokens, replaces
    /// unsafe ones, deletes revoked ones, and returns the table the link
    /// checks requests against. `uid` is the user the files must belong to.
    pub fn sync(dir: &Path, agents: &[HostId], uid: u32) -> Result<(Self, Synced)> {
        crate::room::socket::prepare_dir(dir, uid)?;
        let mut synced = Synced::default();
        let mut entries = Vec::with_capacity(agents.len());
        for agent in agents {
            let path = token_path(dir, agent);
            let token = match read_token(&path, uid)? {
                Existing::Usable(token) => token,
                Existing::Missing => {
                    synced.created.push(agent.clone());
                    issue(&path)?
                }
                Existing::Unsafe => {
                    synced.replaced.push(agent.clone());
                    issue(&path)?
                }
            };
            entries.push((agent.clone(), token));
        }
        synced.revoked = revoke_others(dir, agents)?;
        Ok((Self { entries }, synced))
    }

    /// The agent `presented` is the token of, if any. Every agent's token is
    /// compared, in constant time, whatever matched first.
    pub fn agent_for(&self, presented: &str) -> Option<&HostId> {
        let presented = presented.as_bytes();
        let mut found = None;
        for (name, token) in &self.entries {
            let expected = token.expose().as_bytes();
            // Length is not secret (always 64), so a mismatch may return early
            // inside `ct_eq`; equal lengths compare in constant time.
            let matched = !expected.is_empty() && bool::from(presented.ct_eq(expected));
            if matched {
                found = Some(name);
            }
        }
        found
    }

    pub fn names(&self) -> impl Iterator<Item = &HostId> {
        self.entries.iter().map(|(name, _)| name)
    }

    /// A table over fixed tokens, for tests elsewhere in the crate.
    #[cfg(test)]
    pub(crate) fn fixed(entries: &[(&str, &str)]) -> Self {
        Self {
            entries: entries
                .iter()
                .filter_map(|(name, token)| {
                    Some((
                        HostId::try_from(*name).ok()?,
                        SecretToken::new((*token).to_owned()),
                    ))
                })
                .collect(),
        }
    }
}

/// Where `agent`'s token lives: `<dir>/<agent>.token`. A `HostId` is
/// `[a-z0-9-]`, so the name can never leave the directory.
pub fn token_path(dir: &Path, agent: &HostId) -> PathBuf {
    dir.join(format!("{agent}.{EXTENSION}"))
}

/// What a token file's mode says about it, for `doctor`.
pub fn describe_file(path: &Path) -> String {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == ErrorKind::NotFound => {
            "missing (written when `sealmesh daemon` starts)".to_owned()
        }
        Err(error) => format!("unreadable: {error}"),
        Ok(meta) if !meta.file_type().is_file() => "NOT A REGULAR FILE".to_owned(),
        Ok(meta) => {
            let mode = meta.permissions().mode() & 0o777;
            if mode & 0o077 == 0 {
                format!("{mode:04o} ok")
            } else {
                format!("{mode:04o} TOO OPEN (replaced when `sealmesh daemon` starts)")
            }
        }
    }
}

enum Existing {
    Usable(SecretToken),
    Missing,
    /// Readable or writable by someone else, not this user's, or not a
    /// well-formed token: replaced.
    Unsafe,
}

fn read_token(path: &Path, uid: u32) -> Result<Existing> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Existing::Missing),
        Err(error) => {
            return Err(error).with_context(|| format!("reading {}", path.display()));
        }
    };
    if !meta.file_type().is_file() {
        bail!(
            "{} is not a regular file (a symlink or a directory is there), so it was left \
             alone and the agent link was not started. Remove it and start the daemon again",
            path.display()
        );
    }
    if meta.uid() != uid || meta.permissions().mode() & 0o077 != 0 {
        return Ok(Existing::Unsafe);
    }
    if meta.len() > 256 {
        return Ok(Existing::Unsafe);
    }
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let token = text.trim();
    if is_well_formed(token) {
        Ok(Existing::Usable(SecretToken::new(token.to_owned())))
    } else {
        Ok(Existing::Unsafe)
    }
}

fn is_well_formed(token: &str) -> bool {
    token.len() == TOKEN_BYTES * 2
        && token
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Writes a fresh token to `path`, 0600 from the moment it exists: written
/// beside it under a staging name and renamed into place, so a reader never
/// sees half a token.
fn issue(path: &Path) -> Result<SecretToken> {
    let mut bytes = [0_u8; TOKEN_BYTES];
    OsRng
        .try_fill_bytes(&mut bytes)
        .context("reading randomness for an agent token")?;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();

    let staging = path.with_extension(format!("{EXTENSION}.new"));
    let _ = std::fs::remove_file(&staging);
    {
        use std::io::Write as _;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&staging)
            .with_context(|| format!("creating {}", staging.display()))?;
        file.write_all(hex.as_bytes())
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_all())
            .with_context(|| format!("writing {}", staging.display()))?;
    }
    std::fs::rename(&staging, path)
        .with_context(|| format!("moving an agent token into {}", path.display()))?;
    Ok(SecretToken::new(hex))
}

/// Deletes every `*.token` file in `dir` whose name is not a configured
/// agent. Only regular files; anything else is left where it is.
fn revoke_others(dir: &Path, agents: &[HostId]) -> Result<Vec<String>> {
    let entries = std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))?;
    let mut revoked = Vec::new();
    for entry in entries.take(MAX_ENTRIES).flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|ext| ext != EXTENSION) {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        if agents.iter().any(|agent| agent.as_str() == stem) {
            continue;
        }
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.file_type().is_file() {
            continue;
        }
        std::fs::remove_file(&path).with_context(|| format!("revoking {}", path.display()))?;
        revoked.push(stem.to_owned());
    }
    revoked.sort();
    Ok(revoked)
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

    fn scratch(tag: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("sealmesh-tokens-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join("agents")
    }

    fn names(list: &[&str]) -> Vec<HostId> {
        list.iter().map(|n| HostId::try_from(*n).unwrap()).collect()
    }

    fn uid() -> u32 {
        crate::room::socket::own_uid().unwrap()
    }

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[tokio::test]
    async fn each_agent_gets_a_private_token_that_survives_a_restart() {
        let dir = scratch("issue");
        let agents = names(&["grokbot", "pagerbot"]);
        let (tokens, synced) = Tokens::sync(&dir, &agents, uid()).unwrap();
        assert_eq!(synced.created, agents);
        assert_eq!(mode(&dir), 0o700);
        let grok = token_path(&dir, &agents[0]);
        assert_eq!(mode(&grok), 0o600);
        let written = std::fs::read_to_string(&grok).unwrap();
        assert!(is_well_formed(written.trim()), "{written}");
        assert_eq!(
            tokens.agent_for(written.trim()).map(HostId::as_str),
            Some("grokbot")
        );

        // A restart keeps the same token.
        let (again, synced) = Tokens::sync(&dir, &agents, uid()).unwrap();
        assert_eq!(synced, Synced::default());
        assert_eq!(
            again.agent_for(written.trim()).map(HostId::as_str),
            Some("grokbot")
        );
        std::fs::remove_dir_all(dir.parent().unwrap()).ok();
    }

    #[tokio::test]
    async fn a_removed_agents_token_is_deleted_and_stops_working() {
        let dir = scratch("revoke");
        let (_, _) = Tokens::sync(&dir, &names(&["grokbot", "pagerbot"]), uid()).unwrap();
        let pager = token_path(&dir, &HostId::try_from("pagerbot").unwrap());
        let old = std::fs::read_to_string(&pager).unwrap();
        // A declared-agent record in the same directory is not a token.
        let record = dir.join("job-1.json");
        std::fs::write(&record, "{}").unwrap();

        let (tokens, synced) = Tokens::sync(&dir, &names(&["grokbot"]), uid()).unwrap();
        assert_eq!(synced.revoked, ["pagerbot"]);
        assert!(!pager.exists(), "a revoked token file is gone");
        assert_eq!(tokens.agent_for(old.trim()), None);
        assert!(record.exists(), "a record that is not a token was touched");
        std::fs::remove_dir_all(dir.parent().unwrap()).ok();
    }

    #[tokio::test]
    async fn a_shared_or_malformed_token_is_replaced() {
        let dir = scratch("replace");
        let agents = names(&["grokbot"]);
        let (_, _) = Tokens::sync(&dir, &agents, uid()).unwrap();
        let path = token_path(&dir, &agents[0]);
        let shared = std::fs::read_to_string(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let (tokens, synced) = Tokens::sync(&dir, &agents, uid()).unwrap();
        assert_eq!(synced.replaced, agents);
        assert_eq!(mode(&path), 0o600);
        assert_eq!(
            tokens.agent_for(shared.trim()),
            None,
            "a leaked token still works"
        );

        std::fs::write(&path, "short").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let (_, synced) = Tokens::sync(&dir, &agents, uid()).unwrap();
        assert_eq!(synced.replaced, agents);
        std::fs::remove_dir_all(dir.parent().unwrap()).ok();
    }

    #[tokio::test]
    async fn a_symlink_where_a_token_belongs_is_left_alone_and_refused() {
        let dir = scratch("link");
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.parent().unwrap().join("elsewhere");
        std::fs::write(&target, "x").unwrap();
        let agents = names(&["grokbot"]);
        std::os::unix::fs::symlink(&target, token_path(&dir, &agents[0])).unwrap();
        assert!(Tokens::sync(&dir, &agents, uid()).is_err());
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "x");
        std::fs::remove_dir_all(dir.parent().unwrap()).ok();
    }

    #[test]
    fn lookup_matches_exactly_one_agent_and_nothing_else() {
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        let tokens = Tokens::fixed(&[("grokbot", &a), ("pagerbot", &b)]);
        assert_eq!(tokens.agent_for(&a).map(HostId::as_str), Some("grokbot"));
        assert_eq!(tokens.agent_for(&b).map(HostId::as_str), Some("pagerbot"));
        for wrong in ["", "a", &"a".repeat(63), &"A".repeat(64), &"c".repeat(64)] {
            assert_eq!(tokens.agent_for(wrong), None, "{wrong}");
        }
        assert!(!format!("{tokens:?}").contains(&a), "a token rendered");
    }
}
