//! Where the handshake credential comes from, by mesh mode.
//!
//! `Hello { role, token }` carries an opaque [`SecretToken`] in every mode, so
//! the wire is the same whichever router answers it. What changes is who
//! vouches for it:
//!
//! - `cloud`: [`CredentialSource::GhCli`], the GitHub token `gh` (or
//!   `GH_TOKEN`/`GITHUB_TOKEN`) hands over, exactly as before modes existed.
//! - `std`: [`CredentialSource::LocalSecret`], a per-boot secret the embedded
//!   router writes to a 0600 file under `$XDG_STATE_HOME/sealmesh/router/`.
//!   Reading it proves the caller is the OS user that started the router.
//! - `otw`: [`CredentialSource::None`]. Identity comes from the tailnet (the
//!   router asks Tailscale who is on the other end), so the token is empty.
//!
//! Nothing here stores or logs a token (rules 6, 7): each call reads it fresh,
//! and the caller moves it into the `Hello` frame and drops it.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use sealmesh_proto::SecretToken;

use crate::{config::Mode, github_auth};

/// The longest a local secret file may be. The router writes 64 hex
/// characters; anything much longer is not one of ours.
const MAX_SECRET_BYTES: u64 = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialSource {
    /// The GitHub CLI's token (cloud mode).
    GhCli,
    /// The embedded router's per-boot secret at this path (std mode).
    LocalSecret(PathBuf),
    /// No credential: the network says who this is (otw mode).
    None,
}

impl CredentialSource {
    pub fn for_mode(mode: Mode) -> Result<Self> {
        Ok(match mode {
            Mode::Cloud => Self::GhCli,
            Mode::Std => {
                Self::LocalSecret(crate::paths::local_router_secret().ok_or_else(|| {
                    anyhow!("no home directory to find the local router's secret under")
                })?)
            }
            Mode::Otw => Self::None,
        })
    }

    /// The token to put in `Hello`. Fetched on every call, never cached.
    pub fn token(&self) -> Result<SecretToken> {
        match self {
            Self::GhCli => github_auth::token(),
            Self::LocalSecret(path) => read_local_secret(path),
            Self::None => Ok(SecretToken::new(String::new())),
        }
    }

    /// What to tell someone whose handshake was refused.
    pub fn refusal_hint(&self) -> &'static str {
        match self {
            Self::GhCli => {
                "the router refused this login. Check `gh auth status`, and that you are an \
                 active member of the GitHub organisation this router serves"
            }
            Self::LocalSecret(_) => {
                "the local router refused this machine's secret. It changes each time the \
                 router starts; restart `sealmesh daemon` and try again"
            }
            Self::None => {
                "the router refused this connection. Check that this machine is on the \
                 router's tailnet"
            }
        }
    }
}

/// Reads the embedded router's secret. Refuses a file anyone but its owner
/// can read or write: a secret others can see proves nothing about who holds
/// it, and one others can write could be swapped for theirs.
pub fn read_local_secret(path: &Path) -> Result<SecretToken> {
    let meta = std::fs::metadata(path).with_context(|| {
        format!(
            "reading the local router's secret at {}. Is `sealmesh daemon` running in std mode?",
            path.display()
        )
    })?;
    if !meta.is_file() {
        bail!("{} is not a file", path.display());
    }
    if meta.len() > MAX_SECRET_BYTES {
        bail!("{} is too large to be a router secret", path.display());
    }
    refuse_shared(path, &meta)?;
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading the local router's secret at {}", path.display()))?;
    let secret = text.trim();
    if secret.is_empty() {
        bail!("the local router's secret at {} is empty", path.display());
    }
    Ok(SecretToken::new(secret.to_owned()))
}

#[cfg(unix)]
fn refuse_shared(path: &Path, meta: &std::fs::Metadata) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let mode = meta.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        bail!(
            "{} is mode {mode:o}; a router secret must be readable by its owner alone (0600)",
            path.display()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn refuse_shared(_path: &Path, _meta: &std::fs::Metadata) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("sealmesh-credential-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn each_mode_picks_its_source() {
        assert_eq!(
            CredentialSource::for_mode(Mode::Cloud).unwrap(),
            CredentialSource::GhCli
        );
        assert_eq!(
            CredentialSource::for_mode(Mode::Otw).unwrap(),
            CredentialSource::None
        );
        if let Some(path) = crate::paths::local_router_secret() {
            assert_eq!(
                CredentialSource::for_mode(Mode::Std).unwrap(),
                CredentialSource::LocalSecret(path)
            );
        }
    }

    #[test]
    fn otw_sends_an_empty_token() {
        assert_eq!(CredentialSource::None.token().unwrap().expose(), "");
    }

    #[cfg(unix)]
    #[test]
    fn a_private_secret_file_is_read_and_a_shared_one_refused() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = scratch("perm");
        let path = dir.join("secret");
        std::fs::write(&path, "abc123\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let token = CredentialSource::LocalSecret(path.clone()).token().unwrap();
        assert_eq!(token.expose(), "abc123");

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let error = read_local_secret(&path).unwrap_err();
        assert!(format!("{error:#}").contains("0600"), "{error:#}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_missing_secret_says_where_it_looked() {
        let path = scratch("missing").join("nope");
        let error = read_local_secret(&path).unwrap_err();
        assert!(format!("{error:#}").contains("nope"), "{error:#}");
    }
}
