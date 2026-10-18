use sealmesh_proto::SecretToken;
use sealmesh_wire::{HostId, RepoSlug};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Denied {
    #[error("missing or malformed handshake")]
    NoCredential,
    #[error("not authorised to connect")]
    NotAuthorised,
}

/// What one caller's own credential proves about one repository.
///
/// Three cases and not a `bool`, for the same reason `HostState` is three
/// cases: "GitHub said no" and "GitHub could not be asked" are different facts,
/// and collapsing them lets an outage read as a clean negative. Only
/// `Readable` permits anything. The distinction is for the server's own
/// judgement — never cache `Unverifiable` as a negative — and never for the
/// caller, who gets [`authorise_read`]'s single refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoAccess {
    Readable,
    /// GitHub answered, and the answer is no. A repo that does not exist and
    /// a private repo this token cannot see are both a 404 from GitHub, so
    /// they are both this — nobody, the server included, can tell them apart.
    NotReadable,
    /// The check broke: network, timeout, rate limit, an oversized or
    /// unparseable body, or an answer about a different repository.
    Unverifiable,
}

/// Where one pull request is in its life, as GitHub reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PullState {
    Open,
    /// Closed without merging.
    Closed,
    Merged,
}

/// As much of one pull request as the archive-on-merge check reads: the
/// branch it was opened from, whether it targets the repository's default
/// branch, and its state. Server-side only — nothing here reaches a frame, and
/// the branch name never reaches a log line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullSummary {
    pub head_ref: String,
    /// Whether its base is the repository's default branch. A pull request
    /// merged into anything else — the part below it in a stack — has not
    /// landed the work.
    pub into_default_branch: bool,
    pub state: PullState,
}

/// A repository's recent pull requests could not be listed: network, timeout,
/// rate limit, a repository the token cannot see, or a body that does not
/// settle the question. Distinct from an empty list, which is GitHub saying
/// there are none (core rule 38).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PullsUnverifiable;

pub trait Directory: Send + Sync {
    fn login(&self, token: &SecretToken) -> impl Future<Output = Result<String, Denied>> + Send;

    fn is_member(&self, token: &SecretToken) -> impl Future<Output = Result<bool, Denied>> + Send;

    /// Can this token read `repo`, asked with the caller's own token so it can
    /// only ever resolve access the caller has.
    ///
    /// The default is `Unverifiable`: a directory that has not been taught to
    /// resolve access refuses every read rather than granting one. Kept
    /// deliberately — `room_access` tests an untaught directory against it,
    /// and every directory the router actually runs (`Github`, and the cache
    /// over it) overrides it.
    fn can_read(
        &self,
        _token: &SecretToken,
        _repo: &RepoSlug,
    ) -> impl Future<Output = RepoAccess> + Send {
        async { RepoAccess::Unverifiable }
    }

    /// What [`can_read`](Self::can_read) would answer for `repo` without
    /// asking GitHub, when that is already known — a cache hit.
    ///
    /// The fan-out charges each reader for checks that reach GitHub and not
    /// for ones that do not (`check_budget`), and this is how it tells the two
    /// apart before making the call. The default is `None`: nothing is known,
    /// so every check is paid for, which is the safe direction.
    fn known_access(&self, _token: &SecretToken, _repo: &RepoSlug) -> Option<RepoAccess> {
        None
    }

    /// `repo`'s most recently updated pull requests, at most
    /// [`RECENT_PULLS`](crate::github::RECENT_PULLS) of them, listed with the
    /// caller's own token — the acting connection's, never another's.
    ///
    /// The default is `Err(PullsUnverifiable)`: a directory that has not been
    /// taught to list pull requests leaves every ticket room open, the fail-open
    /// direction for archive (core rule 38). Every directory the router runs
    /// (`Github`, and the cache over it) overrides it.
    fn recent_pulls(
        &self,
        _token: &SecretToken,
        _repo: &RepoSlug,
    ) -> impl Future<Output = Result<Vec<PullSummary>, PullsUnverifiable>> + Send {
        async { Err(PullsUnverifiable) }
    }
}

/// The read decision, failing closed: anything but a confirmed `Readable` is
/// the same refusal, so a prober learns nothing about whether a repo exists or
/// whether GitHub was reachable.
pub async fn authorise_read<D: Directory>(
    directory: &D,
    token: &SecretToken,
    repo: &RepoSlug,
) -> Result<(), Denied> {
    match directory.can_read(token, repo).await {
        RepoAccess::Readable => Ok(()),
        RepoAccess::NotReadable => Err(Denied::NotAuthorised),
        RepoAccess::Unverifiable => {
            // Rule 28. Refused exactly like a no, but not silently: during a
            // GitHub outage every read is refused, and without this line the
            // server keeps no trace of why. The repo is a validated slug; the
            // token and GitHub's response bytes never reach this line.
            eprintln!("sealmesh-server: could not verify read access to {repo}; refused");
            Err(Denied::NotAuthorised)
        }
    }
}

pub async fn authenticate<D: Directory>(
    directory: &D,
    token: &SecretToken,
) -> Result<HostId, Denied> {
    if !directory.is_member(token).await? {
        return Err(Denied::NotAuthorised);
    }
    let login = directory.login(token).await?;
    host_from_login(&login)
}

const MAX_LOGIN_LEN: usize = 39;

pub fn host_from_login(login: &str) -> Result<HostId, Denied> {
    if login.len() > MAX_LOGIN_LEN {
        return Err(Denied::NotAuthorised);
    }

    let mut out = String::with_capacity(login.len());
    for c in login.chars() {
        if c.is_ascii_alphanumeric() {
            out.extend(c.to_lowercase());
        } else if !out.is_empty() {
            out.push('-');
        }
    }
    while out.ends_with('-') {
        out.pop();
    }

    HostId::try_from(out.as_str()).map_err(|_| Denied::NotAuthorised)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    struct Stub {
        login: &'static str,
        member: bool,
    }

    impl Directory for Stub {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Ok(self.login.to_owned())
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(self.member)
        }
    }

    struct Rejects;

    impl Directory for Rejects {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Err(Denied::NotAuthorised)
        }
    }

    struct Grants {
        readable: &'static str,
    }

    impl Directory for Grants {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Ok("sandbank".to_owned())
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(true)
        }
        async fn can_read(&self, _token: &SecretToken, repo: &RepoSlug) -> RepoAccess {
            if repo.as_str() == self.readable {
                RepoAccess::Readable
            } else {
                RepoAccess::NotReadable
            }
        }
    }

    struct Outage;

    impl Directory for Outage {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn can_read(&self, _token: &SecretToken, _repo: &RepoSlug) -> RepoAccess {
            RepoAccess::Unverifiable
        }
    }

    fn token() -> SecretToken {
        SecretToken::new("ghp_example".to_owned())
    }

    fn repo(slug: &str) -> RepoSlug {
        RepoSlug::try_from(slug).unwrap()
    }

    #[tokio::test]
    async fn a_repo_the_caller_can_read_is_authorised() {
        let directory = Grants {
            readable: "example-org/sealmesh",
        };
        assert_eq!(
            authorise_read(&directory, &token(), &repo("example-org/sealmesh")).await,
            Ok(())
        );
    }

    #[tokio::test]
    async fn a_repo_the_caller_cannot_read_is_refused() {
        let directory = Grants {
            readable: "example-org/sealmesh",
        };
        assert_eq!(
            authorise_read(&directory, &token(), &repo("example-org/example-repo")).await,
            Err(Denied::NotAuthorised)
        );
    }

    #[tokio::test]
    async fn an_unverifiable_answer_is_refused_the_same_way_as_a_no() {
        let no = authorise_read(
            &Grants {
                readable: "example-org/sealmesh",
            },
            &token(),
            &repo("example-org/example-repo"),
        )
        .await;
        let unverifiable =
            authorise_read(&Outage, &token(), &repo("example-org/example-repo")).await;
        assert_eq!(unverifiable, Err(Denied::NotAuthorised), "failed open");
        assert_eq!(
            no, unverifiable,
            "an outage and a refusal must be indistinguishable to a caller"
        );
    }

    #[tokio::test]
    async fn a_directory_that_cannot_resolve_access_grants_nothing() {
        let directory = Stub {
            login: "sandbank",
            member: true,
        };
        assert_eq!(
            directory
                .can_read(&token(), &repo("example-org/sealmesh"))
                .await,
            RepoAccess::Unverifiable
        );
        assert_eq!(
            authorise_read(&directory, &token(), &repo("example-org/sealmesh")).await,
            Err(Denied::NotAuthorised)
        );
    }

    #[tokio::test]
    async fn the_host_comes_from_the_github_login() {
        let host = authenticate(
            &Stub {
                login: "SandbankDartCedar",
                member: true,
            },
            &token(),
        )
        .await
        .unwrap();
        assert_eq!(host.as_str(), "sandbankdartcedar");
    }

    #[tokio::test]
    async fn a_valid_token_from_outside_the_org_is_refused() {
        assert_eq!(
            authenticate(
                &Stub {
                    login: "someone-else",
                    member: false
                },
                &token()
            )
            .await,
            Err(Denied::NotAuthorised)
        );
    }

    #[tokio::test]
    async fn an_invalid_token_is_refused_the_same_way() {
        assert_eq!(
            authenticate(&Rejects, &token()).await,
            Err(Denied::NotAuthorised),
            "the two cases must be indistinguishable to a caller"
        );
    }

    #[test]
    fn two_logins_derive_two_hosts() {
        let a = host_from_login("sandbank").unwrap();
        let b = host_from_login("shelter").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn logins_are_folded_into_the_host_charset() {
        for (login, want) in [
            ("sandbank", "sandbank"),
            ("SandbankDart", "sandbankdart"),
            ("example-org", "example-org"),
            ("a-b-c", "a-b-c"),
        ] {
            assert_eq!(host_from_login(login).unwrap().as_str(), want, "{login}");
        }
    }

    #[test]
    fn a_login_that_cannot_become_a_host_is_refused() {
        for login in ["", "---", "..."] {
            assert_eq!(
                host_from_login(login),
                Err(Denied::NotAuthorised),
                "accepted {login:?}"
            );
        }
        assert_eq!(
            host_from_login(&"a".repeat(200)),
            Err(Denied::NotAuthorised)
        );
    }

    #[test]
    fn two_long_logins_sharing_a_prefix_cannot_become_the_same_host() {
        let victim = format!("{}atlas", "a".repeat(32));
        let impostor = format!("{}magenta", "a".repeat(32));
        assert_eq!(victim.len(), 37);
        assert_eq!(impostor.len(), 39);

        let collision = HostId::try_from("a".repeat(32).as_str()).unwrap();

        let resolved: Vec<HostId> = [&victim, &impostor]
            .into_iter()
            .filter_map(|login| host_from_login(login).ok())
            .collect();
        assert!(
            resolved.is_empty(),
            "a login too long for HostId was truncated into {resolved:?}, \
             which collides on {collision}"
        );
    }

    #[test]
    fn a_login_longer_than_github_issues_is_refused_before_normalising() {
        let longest_real = "a".repeat(MAX_LOGIN_LEN);
        assert_eq!(host_from_login(&longest_real), Err(Denied::NotAuthorised));
        assert_eq!(
            host_from_login(&"a".repeat(MAX_LOGIN_LEN + 1)),
            Err(Denied::NotAuthorised)
        );
    }

    #[test]
    fn a_login_that_fits_is_unchanged_by_the_new_refusal() {
        let longest_that_fits = "a".repeat(32);
        assert_eq!(
            host_from_login(&longest_that_fits).unwrap().as_str(),
            longest_that_fits
        );
        assert_eq!(
            host_from_login("SandbankDartCedar").unwrap().as_str(),
            "sandbankdartcedar"
        );
    }
}
