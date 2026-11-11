//! One [`Directory`] type for every mode, without `dyn`.
//!
//! `Directory` returns `impl Future` (RPITIT), so it is not object-safe and
//! `App<D>` is generic over it. A router that picks its directory at runtime,
//! by mode, uses this enum instead: each method matches and delegates.
//!
//! - `Github`: cloud mode, the access cache over the GitHub resolver.
//! - `LocalOwner`: std mode, the embedded router's per-boot secret.
//!
// TODO(otw): add `Tailscale(..)` here; identity comes from Tailscale whois on
// the connection, not from the (empty) token.

use sealmesh_proto::SecretToken;
use sealmesh_wire::RepoSlug;

use crate::{
    access_cache::CachingDirectory,
    auth::{Denied, Directory, PullSummary, PullsUnverifiable, RepoAccess},
    github::Github,
    local_owner::LocalOwner,
};

pub enum ModeDirectory {
    Github(CachingDirectory<Github>),
    LocalOwner(LocalOwner),
}

impl Directory for ModeDirectory {
    async fn login(&self, token: &SecretToken) -> Result<String, Denied> {
        match self {
            Self::Github(directory) => directory.login(token).await,
            Self::LocalOwner(directory) => directory.login(token).await,
        }
    }

    async fn is_member(&self, token: &SecretToken) -> Result<bool, Denied> {
        match self {
            Self::Github(directory) => directory.is_member(token).await,
            Self::LocalOwner(directory) => directory.is_member(token).await,
        }
    }

    async fn can_read(&self, token: &SecretToken, repo: &RepoSlug) -> RepoAccess {
        match self {
            Self::Github(directory) => directory.can_read(token, repo).await,
            Self::LocalOwner(directory) => directory.can_read(token, repo).await,
        }
    }

    fn known_access(&self, token: &SecretToken, repo: &RepoSlug) -> Option<RepoAccess> {
        match self {
            Self::Github(directory) => directory.known_access(token, repo),
            Self::LocalOwner(directory) => directory.known_access(token, repo),
        }
    }

    async fn recent_pulls(
        &self,
        token: &SecretToken,
        repo: &RepoSlug,
    ) -> Result<Vec<PullSummary>, PullsUnverifiable> {
        match self {
            Self::Github(directory) => directory.recent_pulls(token, repo).await,
            Self::LocalOwner(directory) => directory.recent_pulls(token, repo).await,
        }
    }
}

impl From<CachingDirectory<Github>> for ModeDirectory {
    fn from(directory: CachingDirectory<Github>) -> Self {
        Self::Github(directory)
    }
}

impl From<LocalOwner> for ModeDirectory {
    fn from(directory: LocalOwner) -> Self {
        Self::LocalOwner(directory)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[tokio::test]
    async fn the_local_owner_arm_delegates() {
        let directory =
            ModeDirectory::from(LocalOwner::new("me", SecretToken::new("k".to_owned())).unwrap());
        let good = SecretToken::new("k".to_owned());
        let bad = SecretToken::new("j".to_owned());
        let repo = RepoSlug::try_from("o/r").unwrap();
        assert_eq!(directory.is_member(&good).await, Ok(true));
        assert_eq!(directory.is_member(&bad).await, Ok(false));
        assert_eq!(directory.login(&good).await, Ok("me".to_owned()));
        assert_eq!(directory.can_read(&good, &repo).await, RepoAccess::Readable);
        assert_eq!(directory.known_access(&good, &repo), None);
        assert!(directory.recent_pulls(&good, &repo).await.is_err());
    }
}
