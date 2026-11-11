//! The std-mode directory: one machine, one OS user, one secret.
//!
//! An embedded router (std mode) has no GitHub to ask. Whoever can read the
//! per-boot secret it writes is the OS user that started it, since the file is
//! 0600 inside a 0700 directory, and that user is the only member. There are
//! no repo permissions to resolve on one person's own machine, so every repo
//! reads as `Readable` to that member and to nobody else. Pull requests are
//! never listed, so ticket-room archiving stays off (its fail-open direction).

use std::path::Path;

use anyhow::{Context, Result, bail};
use rand::{TryRngCore as _, rngs::OsRng};
use sealmesh_proto::SecretToken;
use sealmesh_wire::{HostId, RepoSlug};
use subtle::ConstantTimeEq as _;

use crate::auth::{Denied, Directory, RepoAccess, host_from_login};

/// Bytes of randomness in a per-boot secret (64 hex characters).
const SECRET_BYTES: usize = 32;

/// The directory an embedded std-mode router runs with.
pub struct LocalOwner {
    login: String,
    secret: SecretToken,
}

impl std::fmt::Debug for LocalOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalOwner")
            .field("login", &self.login)
            .field("secret", &self.secret)
            .finish()
    }
}

impl LocalOwner {
    /// A directory whose one member is `user`, proved by `secret`. `user` is
    /// folded the way a GitHub login is ([`host_from_login`]), and refused if
    /// nothing usable is left.
    pub fn new(user: &str, secret: SecretToken) -> Result<Self, Denied> {
        let login = host_from_login(user)?.to_string();
        Ok(Self { login, secret })
    }

    /// Starts a router's boot: a fresh secret, written 0600 to `path` (its
    /// directory created 0700), and a directory that admits whoever presents
    /// it as the current OS user. The previous boot's secret stops working
    /// the moment this one is written.
    pub fn boot(path: &Path) -> Result<Self> {
        let secret = fresh_secret()?;
        write_secret(path, &secret)?;
        let user = os_user();
        Self::new(&user, secret).map_err(|_| {
            anyhow::anyhow!(
                "the OS user name {user:?} does not fold into a sealmesh host name; set USER"
            )
        })
    }

    /// The member's host, as `Welcome` reports it.
    pub fn host(&self) -> Result<HostId, Denied> {
        host_from_login(&self.login)
    }

    fn holds(&self, token: &SecretToken) -> bool {
        let presented = token.expose().as_bytes();
        let expected = self.secret.expose().as_bytes();
        // Length is not secret (always 64), so a length mismatch may return
        // early; equal lengths compare in constant time.
        !expected.is_empty() && bool::from(presented.ct_eq(expected))
    }
}

impl Directory for LocalOwner {
    async fn login(&self, token: &SecretToken) -> Result<String, Denied> {
        if self.holds(token) {
            Ok(self.login.clone())
        } else {
            Err(Denied::NotAuthorised)
        }
    }

    async fn is_member(&self, token: &SecretToken) -> Result<bool, Denied> {
        Ok(self.holds(token))
    }

    async fn can_read(&self, token: &SecretToken, _repo: &RepoSlug) -> RepoAccess {
        if self.holds(token) {
            RepoAccess::Readable
        } else {
            RepoAccess::NotReadable
        }
    }

    // `recent_pulls` keeps the trait default, `Err(PullsUnverifiable)`: no
    // pull requests are listed, so archive-on-merge never fires here.
}

/// The OS user running this process, from `USER` or `LOGNAME`. `local` when
/// neither is set, which still folds into a host name.
pub fn os_user() -> String {
    ["USER", "LOGNAME"]
        .into_iter()
        .filter_map(|var| std::env::var(var).ok())
        .map(|value| value.trim().to_owned())
        .find(|value| !value.is_empty())
        .unwrap_or_else(|| "local".to_owned())
}

fn fresh_secret() -> Result<SecretToken> {
    let mut bytes = [0_u8; SECRET_BYTES];
    OsRng
        .try_fill_bytes(&mut bytes)
        .context("reading randomness for the local router's secret")?;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Ok(SecretToken::new(hex))
}

/// Writes `secret` to `path`, readable by its owner alone. Written beside it
/// and renamed into place, so a reader never sees half a secret, and the file
/// is created 0600 rather than narrowed after the fact.
fn write_secret(path: &Path, secret: &SecretToken) -> Result<()> {
    let Some(dir) = path.parent() else {
        bail!("{} has no parent directory", path.display());
    };
    create_private_dir(dir)?;
    let staging = path.with_extension("new");
    let _ = std::fs::remove_file(&staging);
    write_private(&staging, secret.expose().as_bytes())
        .with_context(|| format!("writing {}", staging.display()))?;
    std::fs::rename(&staging, path)
        .with_context(|| format!("moving the router secret into {}", path.display()))?;
    Ok(())
}

#[cfg(unix)]
fn create_private_dir(dir: &Path) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("making {} private", dir.display()))
}

#[cfg(not(unix))]
fn create_private_dir(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::{io::Write as _, os::unix::fs::OpenOptionsExt as _};
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn owner() -> LocalOwner {
        LocalOwner::new("Santa.Maria", SecretToken::new("s3cret".to_owned())).unwrap()
    }

    fn repo() -> RepoSlug {
        RepoSlug::try_from("example-org/example-repo").unwrap()
    }

    #[tokio::test]
    async fn the_secret_makes_a_member_and_names_the_os_user() {
        let owner = owner();
        let token = SecretToken::new("s3cret".to_owned());
        assert_eq!(owner.is_member(&token).await, Ok(true));
        assert_eq!(owner.login(&token).await, Ok("santa-maria".to_owned()));
        assert_eq!(owner.can_read(&token, &repo()).await, RepoAccess::Readable);
        assert_eq!(owner.host().unwrap().as_str(), "santa-maria");
        assert_eq!(
            crate::auth::authenticate(&owner, &token)
                .await
                .unwrap()
                .as_str(),
            "santa-maria"
        );
    }

    #[tokio::test]
    async fn a_wrong_or_missing_secret_is_refused() {
        let owner = owner();
        for wrong in ["", "s3cre", "s3cret!", "S3CRET"] {
            let token = SecretToken::new(wrong.to_owned());
            assert_eq!(owner.is_member(&token).await, Ok(false), "{wrong:?}");
            assert_eq!(
                owner.login(&token).await,
                Err(Denied::NotAuthorised),
                "{wrong:?}"
            );
            assert_eq!(
                owner.can_read(&token, &repo()).await,
                RepoAccess::NotReadable
            );
            assert!(crate::auth::authenticate(&owner, &token).await.is_err());
        }
    }

    #[tokio::test]
    async fn no_pull_requests_are_listed_so_archiving_stays_off() {
        let token = SecretToken::new("s3cret".to_owned());
        assert!(owner().recent_pulls(&token, &repo()).await.is_err());
    }

    #[test]
    fn an_empty_secret_admits_nobody() {
        let owner = LocalOwner::new("me", SecretToken::new(String::new())).unwrap();
        assert!(!owner.holds(&SecretToken::new(String::new())));
    }

    #[test]
    fn the_secret_never_renders() {
        let rendered = format!("{:?}", owner());
        assert!(!rendered.contains("s3cret"), "{rendered}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn boot_writes_a_fresh_private_secret_each_time() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = std::env::temp_dir().join(format!("sealmesh-boot-{}", std::process::id()));
        let path = dir.join("router/secret");
        let first = LocalOwner::boot(&path).unwrap();
        let written = std::fs::read_to_string(&path).unwrap();
        assert_eq!(written.len(), SECRET_BYTES * 2);
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let dir_mode = std::fs::metadata(path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700);
        let token = SecretToken::new(written.clone());
        assert_eq!(first.is_member(&token).await, Ok(true));

        let second = LocalOwner::boot(&path).unwrap();
        let rewritten = std::fs::read_to_string(&path).unwrap();
        assert_ne!(written, rewritten, "a new boot must mint a new secret");
        assert_eq!(second.is_member(&token).await, Ok(false));

        std::fs::remove_dir_all(&dir).ok();
    }
}
