//! One [`Directory`] type for every mode, without `dyn`.
//!
//! `Directory` returns `impl Future` (RPITIT), so it is not object-safe and
//! `App<D>` is generic over it. A router that picks its directory at runtime,
//! by mode, uses this enum instead: each method matches and delegates.
//!
//! - `Github`: cloud mode, the access cache over the GitHub resolver.
//! - `LocalOwner`: std mode, the embedded router's per-boot secret.
//! - `Tailscale`: otw mode; identity comes from Tailscale whois on the
//!   connection's peer address, not from the (empty) token.

use std::net::SocketAddr;

use sealmesh_proto::SecretToken;
use sealmesh_wire::{HostId, RepoSlug};

use crate::{
    access_cache::CachingDirectory,
    auth::{Denied, Directory, PeerIdentity, PullSummary, PullsUnverifiable, RepoAccess},
    github::Github,
    local_owner::LocalOwner,
    tailscale::TailscaleDirectory,
};

pub enum ModeDirectory {
    Github(CachingDirectory<Github>),
    LocalOwner(LocalOwner),
    Tailscale(TailscaleDirectory),
}

impl Directory for ModeDirectory {
    async fn login(&self, token: &SecretToken) -> Result<String, Denied> {
        match self {
            Self::Github(directory) => directory.login(token).await,
            Self::LocalOwner(directory) => directory.login(token).await,
            Self::Tailscale(directory) => directory.login(token).await,
        }
    }

    async fn is_member(&self, token: &SecretToken) -> Result<bool, Denied> {
        match self {
            Self::Github(directory) => directory.is_member(token).await,
            Self::LocalOwner(directory) => directory.is_member(token).await,
            Self::Tailscale(directory) => directory.is_member(token).await,
        }
    }

    async fn can_read(&self, token: &SecretToken, repo: &RepoSlug) -> RepoAccess {
        match self {
            Self::Github(directory) => directory.can_read(token, repo).await,
            Self::LocalOwner(directory) => directory.can_read(token, repo).await,
            Self::Tailscale(directory) => directory.can_read(token, repo).await,
        }
    }

    fn known_access(&self, token: &SecretToken, repo: &RepoSlug) -> Option<RepoAccess> {
        match self {
            Self::Github(directory) => directory.known_access(token, repo),
            Self::LocalOwner(directory) => directory.known_access(token, repo),
            Self::Tailscale(directory) => directory.known_access(token, repo),
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
            Self::Tailscale(directory) => directory.recent_pulls(token, repo).await,
        }
    }

    async fn peer_identity(
        &self,
        peer: Option<SocketAddr>,
    ) -> Option<Result<PeerIdentity, Denied>> {
        match self {
            Self::Github(directory) => directory.peer_identity(peer).await,
            Self::LocalOwner(directory) => directory.peer_identity(peer).await,
            Self::Tailscale(directory) => directory.peer_identity(peer).await,
        }
    }

    fn room_identity(&self, host: &HostId) -> HostId {
        match self {
            Self::Github(directory) => directory.room_identity(host),
            Self::LocalOwner(directory) => directory.room_identity(host),
            Self::Tailscale(directory) => directory.room_identity(host),
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

impl From<TailscaleDirectory> for ModeDirectory {
    fn from(directory: TailscaleDirectory) -> Self {
        Self::Tailscale(directory)
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
        assert_eq!(directory.peer_identity(None).await, None);
        let me = HostId::try_from("me").unwrap();
        assert_eq!(directory.room_identity(&me).as_str(), "claude");
    }

    #[tokio::test]
    async fn the_tailscale_arm_identifies_by_peer_and_never_by_token() {
        let allow =
            crate::tailscale::Allowlist::new(vec!["alice@github".to_owned()], vec![]).unwrap();
        let directory = ModeDirectory::from(TailscaleDirectory::new(
            std::path::PathBuf::from("/nonexistent/tailscaled.sock"),
            allow,
        ));
        let token = SecretToken::new(String::new());
        let repo = RepoSlug::try_from("o/r").unwrap();
        assert_eq!(directory.is_member(&token).await, Ok(false));
        assert_eq!(
            directory.can_read(&token, &repo).await,
            RepoAccess::Readable
        );
        assert_eq!(
            directory.known_access(&token, &repo),
            Some(RepoAccess::Readable)
        );
        assert!(directory.recent_pulls(&token, &repo).await.is_err());
        // tailscaled is not there, so the peer is refused, not waved through.
        assert_eq!(
            directory
                .peer_identity(Some("100.64.0.1:1".parse().unwrap()))
                .await,
            Some(Err(Denied::NotAuthorised))
        );
    }
}
