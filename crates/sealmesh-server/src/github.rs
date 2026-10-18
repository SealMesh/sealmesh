use std::time::Duration;

use sealmesh_proto::SecretToken;
use sealmesh_wire::RepoSlug;
use serde::Deserialize;

use crate::auth::{Denied, Directory, PullState, PullSummary, PullsUnverifiable, RepoAccess};

pub const GITHUB_TIMEOUT: Duration = Duration::from_secs(5);

const API_ROOT: &str = "https://api.github.com";

const USER_AGENT: &str = "sealmesh";

const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// How many of a repository's most recently updated pull requests the
/// archive-on-merge check reads, in one request.
///
/// A ticket's pull request is among the most recently updated at exactly the
/// moment it matters — just after it merges. Fifty covers weeks of a busy
/// repository, and one that has scrolled past them reads as "no merged pull
/// request found", which leaves the room open: the fail-open direction. The
/// reply stays far under [`MAX_RESPONSE_BYTES`] even at GitHub's longest
/// branch names (fifty nodes of about 300 bytes).
pub const RECENT_PULLS: usize = 50;

/// The one GraphQL document this router sends. Fixed text: the repository is a
/// variable, never interpolated, so a slug cannot change what is asked. It
/// selects only the branch and the state of each pull request, which is what
/// keeps the reply small enough to cap — the REST listing carries two full
/// repository objects per pull request and would not fit.
const RECENT_PULLS_QUERY: &str = "query($owner:String!,$name:String!){repository(owner:$owner,\
     name:$name){nameWithOwner defaultBranchRef{name} pullRequests(first:50,orderBy:\
     {field:UPDATED_AT,direction:DESC}){nodes{headRefName baseRefName state}}}}";

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error(
        "{0:?} is not a GitHub organisation name. It is interpolated into an API URL, so a \
         name with a slash or a dot in it asks a different question than the one intended \
         and the membership check stops meaning anything."
    )]
    Org(String),
    #[error("building the HTTP client")]
    Client(#[from] reqwest::Error),
}

fn is_org_name(org: &str) -> bool {
    (1..=39).contains(&org.len())
        && org.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        && !org.starts_with('-')
        && !org.ends_with('-')
}

pub struct Github {
    client: reqwest::Client,
    org: String,
    api_root: String,
}

impl Github {
    pub fn new(org: String) -> Result<Self, BuildError> {
        Self::build(org, API_ROOT.to_owned())
    }

    /// Only a test may point the client somewhere other than GitHub: the token
    /// goes wherever `api_root` says.
    #[cfg(test)]
    fn with_api_root(org: String, api_root: String) -> Result<Self, BuildError> {
        Self::build(org, api_root)
    }

    fn build(org: String, api_root: String) -> Result<Self, BuildError> {
        if !is_org_name(&org) {
            return Err(BuildError::Org(org));
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(GITHUB_TIMEOUT)
                .user_agent(USER_AGENT)
                .build()?,
            org,
            api_root,
        })
    }

    fn repo_url(&self, repo: &RepoSlug) -> String {
        format!("{}/repos/{}/{}", self.api_root, repo.owner(), repo.repo())
    }

    fn get(&self, url: String, token: &SecretToken) -> reqwest::RequestBuilder {
        self.client
            .get(url)
            .header("accept", "application/vnd.github+json")
            .header("x-github-api-version", "2022-11-28")
            .bearer_auth(token.expose())
    }
}

impl Github {
    fn graphql(&self, token: &SecretToken, body: &serde_json::Value) -> reqwest::RequestBuilder {
        self.client
            .post(format!("{}/graphql", self.api_root))
            .header("accept", "application/vnd.github+json")
            .bearer_auth(token.expose())
            .json(body)
    }
}

fn push_capped(buffer: &mut Vec<u8>, chunk: &[u8], cap: usize) -> bool {
    if buffer.len().saturating_add(chunk.len()) > cap {
        return false;
    }
    buffer.extend_from_slice(chunk);
    true
}

async fn read_capped(mut response: reqwest::Response) -> Result<Vec<u8>, Denied> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| Denied::NotAuthorised)? {
        if !push_capped(&mut body, &chunk, MAX_RESPONSE_BYTES) {
            return Err(Denied::NotAuthorised);
        }
    }
    Ok(body)
}

#[derive(Deserialize)]
struct User {
    login: String,
}

#[derive(Deserialize)]
struct Membership {
    state: String,
}

#[derive(Deserialize)]
struct Repository {
    full_name: String,
    #[serde(default)]
    permissions: Option<Permissions>,
}

#[derive(Deserialize)]
struct Permissions {
    pull: bool,
}

/// Judge a `200` body. Called only on bytes `read_capped` already bounded.
fn access_from_body(requested: &RepoSlug, body: &[u8]) -> RepoAccess {
    let Ok(found) = serde_json::from_slice::<Repository>(body) else {
        return RepoAccess::Unverifiable;
    };
    // reqwest follows a rename's 301, so a 200 can describe a repository other
    // than the one asked about. Slugs are case-insensitive on GitHub.
    if !found.full_name.eq_ignore_ascii_case(requested.as_str()) {
        return RepoAccess::Unverifiable;
    }
    match found.permissions {
        Some(Permissions { pull: true }) => RepoAccess::Readable,
        Some(Permissions { pull: false }) => RepoAccess::NotReadable,
        None => RepoAccess::Unverifiable,
    }
}

#[derive(Deserialize)]
struct PullsReply {
    data: Option<PullsData>,
    /// Any error at all makes the listing unverifiable, whatever `data` says:
    /// GraphQL returns partial data beside errors, and a partial list read as
    /// a whole one could miss the open pull request that keeps a room open.
    #[serde(default)]
    errors: Option<Vec<serde::de::IgnoredAny>>,
}

#[derive(Deserialize)]
struct PullsData {
    repository: Option<PullsRepository>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PullsRepository {
    name_with_owner: String,
    /// `null` for a repository with no branches, and absent if GitHub ever
    /// stops sending it. Either way there is no default branch to compare a
    /// merge against, so the listing is unverifiable rather than "no merge".
    default_branch_ref: Option<BranchRef>,
    pull_requests: PullsConnection,
}

#[derive(Deserialize)]
struct BranchRef {
    name: String,
}

#[derive(Deserialize)]
struct PullsConnection {
    nodes: Vec<Option<PullNode>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PullNode {
    head_ref_name: String,
    base_ref_name: String,
    state: String,
}

/// Judge a `200` GraphQL body. Called only on bytes `read_capped` already
/// bounded. Everything that does not settle the question — errors beside the
/// data, no repository, a node GitHub could not resolve, a state this build
/// does not know, a reply about another repository, no default branch — is
/// unverifiable.
fn pulls_from_body(
    requested: &RepoSlug,
    body: &[u8],
) -> Result<Vec<PullSummary>, PullsUnverifiable> {
    let reply: PullsReply = serde_json::from_slice(body).map_err(|_| PullsUnverifiable)?;
    if reply.errors.is_some_and(|errors| !errors.is_empty()) {
        return Err(PullsUnverifiable);
    }
    let repository = reply
        .data
        .and_then(|data| data.repository)
        .ok_or(PullsUnverifiable)?;
    if !repository
        .name_with_owner
        .eq_ignore_ascii_case(requested.as_str())
    {
        return Err(PullsUnverifiable);
    }
    let default_branch = repository
        .default_branch_ref
        .map(|branch| branch.name)
        .filter(|name| !name.is_empty())
        .ok_or(PullsUnverifiable)?;
    let nodes = repository.pull_requests.nodes;
    if nodes.len() > RECENT_PULLS {
        return Err(PullsUnverifiable);
    }
    nodes
        .into_iter()
        .map(|node| {
            let node = node.ok_or(PullsUnverifiable)?;
            let state = match node.state.as_str() {
                "OPEN" => PullState::Open,
                "CLOSED" => PullState::Closed,
                "MERGED" => PullState::Merged,
                _ => return Err(PullsUnverifiable),
            };
            Ok(PullSummary {
                head_ref: node.head_ref_name,
                // Git ref names are case-sensitive, so this is too.
                into_default_branch: node.base_ref_name == default_branch,
                state,
            })
        })
        .collect()
}

impl Directory for Github {
    async fn login(&self, token: &SecretToken) -> Result<String, Denied> {
        let response = self
            .get(format!("{}/user", self.api_root), token)
            .send()
            .await
            .map_err(|_| Denied::NotAuthorised)?;

        if !response.status().is_success() {
            return Err(Denied::NotAuthorised);
        }
        let body = read_capped(response).await?;
        let user: User = serde_json::from_slice(&body).map_err(|_| Denied::NotAuthorised)?;
        if user.login.is_empty() {
            return Err(Denied::NotAuthorised);
        }
        Ok(user.login)
    }

    async fn is_member(&self, token: &SecretToken) -> Result<bool, Denied> {
        let url = format!("{}/user/memberships/orgs/{}", self.api_root, self.org);
        let response = self
            .get(url, token)
            .send()
            .await
            .map_err(|_| Denied::NotAuthorised)?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if !response.status().is_success() {
            return Err(Denied::NotAuthorised);
        }

        let body = read_capped(response).await?;
        let membership: Membership =
            serde_json::from_slice(&body).map_err(|_| Denied::NotAuthorised)?;
        Ok(membership.state == "active")
    }

    /// Only a 404 is a confirmed no. Every other failure — transport, timeout,
    /// 401, 403 (rate limit or SAML), 5xx, an oversized or unparseable body —
    /// is `Unverifiable`, which `authorise_read` refuses all the same.
    async fn can_read(&self, token: &SecretToken, repo: &RepoSlug) -> RepoAccess {
        let Ok(response) = self.get(self.repo_url(repo), token).send().await else {
            return RepoAccess::Unverifiable;
        };
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return RepoAccess::NotReadable;
        }
        if status != reqwest::StatusCode::OK {
            return RepoAccess::Unverifiable;
        }
        let Ok(body) = read_capped(response).await else {
            return RepoAccess::Unverifiable;
        };
        access_from_body(repo, &body)
    }

    /// Anything but a `200` whose capped body settles the question is
    /// unverifiable — a 401, a 403 (rate limit or SAML), a 5xx, a transport
    /// error or a timeout. There is no "confirmed no" here the way a 404 is
    /// for `can_read`: an empty list is only ever GitHub's own answer.
    async fn recent_pulls(
        &self,
        token: &SecretToken,
        repo: &RepoSlug,
    ) -> Result<Vec<PullSummary>, PullsUnverifiable> {
        let body = serde_json::json!({
            "query": RECENT_PULLS_QUERY,
            "variables": { "owner": repo.owner(), "name": repo.repo() },
        });
        let response = self
            .graphql(token, &body)
            .send()
            .await
            .map_err(|_| PullsUnverifiable)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(PullsUnverifiable);
        }
        let body = read_capped(response).await.map_err(|_| PullsUnverifiable)?;
        pulls_from_body(repo, &body)
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

    #[test]
    fn the_membership_url_is_built_from_the_configured_org() {
        let github = Github::new("example-org".to_owned()).unwrap();
        assert_eq!(
            format!("{API_ROOT}/user/memberships/orgs/{}", github.org),
            "https://api.github.com/user/memberships/orgs/example-org"
        );
    }

    #[test]
    fn only_active_membership_counts() {
        let active: Membership = serde_json::from_str(r#"{"state":"active"}"#).unwrap();
        let pending: Membership = serde_json::from_str(r#"{"state":"pending"}"#).unwrap();
        assert_eq!(active.state, "active");
        assert_ne!(pending.state, "active");
    }

    #[test]
    fn a_user_response_without_a_login_is_not_usable() {
        let user: User = serde_json::from_str(r#"{"login":""}"#).unwrap();
        assert!(user.login.is_empty(), "the guard in `login` catches this");
    }

    #[test]
    fn an_org_name_that_could_leave_its_url_segment_is_refused() {
        for hostile in [
            "../../user",
            "example-org/../octocat",
            "example-org/repos",
            "example%2Fsoftware",
            "example software",
            "example.software",
            "",
            "-leading",
            "trailing-",
            &"a".repeat(40),
        ] {
            assert!(
                Github::new(hostile.to_owned()).is_err(),
                "accepted org {hostile:?}"
            );
        }
    }

    #[test]
    fn the_real_org_name_is_accepted() {
        assert!(Github::new("example-org".to_owned()).is_ok());
        assert!(Github::new("a".to_owned()).is_ok());
    }

    #[test]
    fn a_response_body_stops_being_read_at_the_cap() {
        let mut body = Vec::new();
        assert!(push_capped(&mut body, &[b'a'; 8], 16));
        assert!(push_capped(&mut body, &[b'a'; 8], 16));
        assert!(!push_capped(&mut body, &[b'a'; 1], 16), "grew past the cap");
        assert_eq!(body.len(), 16, "a refused chunk must not be partly kept");
    }

    #[test]
    fn one_oversized_chunk_is_refused_whole() {
        let mut body = Vec::new();
        assert!(!push_capped(&mut body, &[b'a'; 4096], 16));
        assert!(body.is_empty());
    }

    #[test]
    fn the_cap_is_far_above_a_real_response_and_far_below_unbounded() {
        const { assert!(MAX_RESPONSE_BYTES >= 8 * 1024) };
        const { assert!(MAX_RESPONSE_BYTES <= 1024 * 1024) };
    }

    const CALLER: &str = "ghp_the_callers_own";

    fn repo(slug: &str) -> RepoSlug {
        RepoSlug::try_from(slug).unwrap()
    }

    fn caller() -> SecretToken {
        SecretToken::new(CALLER.to_owned())
    }

    fn repo_body(full_name: &str, pull: bool) -> String {
        format!(
            r#"{{"full_name":"{full_name}","private":true,"permissions":{{"admin":false,"pull":{pull}}}}}"#
        )
    }

    /// A GitHub that answers `/repos/{owner}/{repo}` the way the real one does:
    /// a private repo is a 404 to any token that cannot see it.
    async fn fake_github() -> Github {
        use axum::{
            extract::Path,
            http::{HeaderMap, StatusCode},
            routing::get,
        };

        async fn repos(
            Path((owner, name)): Path<(String, String)>,
            headers: HeaderMap,
        ) -> (StatusCode, String) {
            let bearer = format!("Bearer {CALLER}");
            if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some(&bearer) {
                return (
                    StatusCode::NOT_FOUND,
                    r#"{"message":"Not Found"}"#.to_owned(),
                );
            }
            let full_name = format!("{owner}/{name}");
            match name.as_str() {
                "sealmesh" => (StatusCode::OK, repo_body(&full_name, true)),
                "pull-denied" => (StatusCode::OK, repo_body(&full_name, false)),
                "renamed" => (StatusCode::OK, repo_body("example-org/elsewhere", true)),
                "no-permissions" => (StatusCode::OK, format!(r#"{{"full_name":"{full_name}"}}"#)),
                "not-json" => (StatusCode::OK, "<html>".to_owned()),
                "rate-limited" => (
                    StatusCode::FORBIDDEN,
                    r#"{"message":"rate limit"}"#.to_owned(),
                ),
                "broken" => (StatusCode::BAD_GATEWAY, String::new()),
                "huge" => {
                    let granting = format!(
                        r#"{{"full_name":"{full_name}","permissions":{{"pull":true}},"pad":"{}"}}"#,
                        "a".repeat(MAX_RESPONSE_BYTES)
                    );
                    (StatusCode::OK, granting)
                }
                _ => (
                    StatusCode::NOT_FOUND,
                    r#"{"message":"Not Found"}"#.to_owned(),
                ),
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = axum::Router::new().route("/repos/{owner}/{repo}", get(repos));
        tokio::spawn(async move { axum::serve(listener, app).await });
        Github::with_api_root("example-org".to_owned(), format!("http://{addr}")).unwrap()
    }

    #[test]
    fn the_repo_url_is_built_from_the_slug_segments() {
        let github = Github::new("example-org".to_owned()).unwrap();
        assert_eq!(
            github.repo_url(&repo("example-org/sealmesh")),
            "https://api.github.com/repos/example-org/sealmesh"
        );
    }

    #[tokio::test]
    async fn a_repo_the_callers_token_can_read_is_readable() {
        let github = fake_github().await;
        assert_eq!(
            github
                .can_read(&caller(), &repo("example-org/sealmesh"))
                .await,
            RepoAccess::Readable
        );
    }

    #[test]
    fn a_slug_differing_only_in_case_is_the_same_repo() {
        assert_eq!(
            access_from_body(
                &repo("Example-org/Sealmesh"),
                repo_body("example-org/sealmesh", true).as_bytes()
            ),
            RepoAccess::Readable
        );
    }

    #[tokio::test]
    async fn a_repo_github_shows_without_pull_is_not_readable() {
        let github = fake_github().await;
        assert_eq!(
            github
                .can_read(&caller(), &repo("example-org/pull-denied"))
                .await,
            RepoAccess::NotReadable
        );
    }

    #[tokio::test]
    async fn a_404_is_not_readable_whether_missing_or_hidden() {
        let github = fake_github().await;
        let missing = github
            .can_read(&caller(), &repo("example-org/does-not-exist"))
            .await;
        let hidden = github
            .can_read(
                &SecretToken::new("ghp_someone_else".to_owned()),
                &repo("example-org/sealmesh"),
            )
            .await;
        assert_eq!(missing, RepoAccess::NotReadable);
        assert_eq!(missing, hidden, "a prober must not tell the two apart");
    }

    #[tokio::test]
    async fn access_is_resolved_with_the_callers_token_and_nothing_else() {
        let github = fake_github().await;
        assert_eq!(
            github
                .can_read(
                    &SecretToken::new("ghp_someone_else".to_owned()),
                    &repo("example-org/sealmesh")
                )
                .await,
            RepoAccess::NotReadable,
            "a token that cannot read the repo was granted it"
        );
    }

    #[tokio::test]
    async fn a_network_error_is_unverifiable_not_a_no() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });
        let github =
            Github::with_api_root("example-org".to_owned(), format!("http://{addr}")).unwrap();
        assert_eq!(
            github
                .can_read(&caller(), &repo("example-org/sealmesh"))
                .await,
            RepoAccess::Unverifiable
        );
    }

    #[tokio::test]
    async fn an_api_failure_is_unverifiable_not_a_no() {
        let github = fake_github().await;
        for slug in ["example-org/rate-limited", "example-org/broken"] {
            assert_eq!(
                github.can_read(&caller(), &repo(slug)).await,
                RepoAccess::Unverifiable,
                "{slug}"
            );
        }
    }

    #[tokio::test]
    async fn an_oversized_response_is_refused_before_it_is_parsed() {
        let github = fake_github().await;
        // The oversized body is valid JSON that grants pull, so a parser that
        // ran first would answer `Readable`.
        assert_eq!(
            github.can_read(&caller(), &repo("example-org/huge")).await,
            RepoAccess::Unverifiable
        );
    }

    #[tokio::test]
    async fn a_body_that_does_not_settle_the_question_is_unverifiable() {
        let github = fake_github().await;
        for slug in [
            "example-org/renamed",
            "example-org/no-permissions",
            "example-org/not-json",
        ] {
            assert_eq!(
                github.can_read(&caller(), &repo(slug)).await,
                RepoAccess::Unverifiable,
                "{slug}"
            );
        }
    }

    fn pulls_body_on(name_with_owner: &str, default_branch: &str, nodes: &str) -> String {
        format!(
            r#"{{"data":{{"repository":{{"nameWithOwner":"{name_with_owner}","defaultBranchRef":{default_branch},"pullRequests":{{"nodes":[{nodes}]}}}}}}}}"#
        )
    }

    fn pulls_body(name_with_owner: &str, nodes: &str) -> String {
        pulls_body_on(name_with_owner, r#"{"name":"main"}"#, nodes)
    }

    /// A GitHub GraphQL endpoint that answers the pull request listing by the
    /// repository named in the request's variables, and only to `CALLER`.
    async fn fake_graphql() -> Github {
        use axum::{
            http::{HeaderMap, StatusCode},
            routing::post,
        };

        async fn graphql(headers: HeaderMap, body: String) -> (StatusCode, String) {
            let bearer = format!("Bearer {CALLER}");
            if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some(&bearer) {
                return (
                    StatusCode::UNAUTHORIZED,
                    r#"{"message":"Bad credentials"}"#.to_owned(),
                );
            }
            let request: serde_json::Value = serde_json::from_str(&body).unwrap();
            // The document is fixed text; the repository only ever arrives as
            // variables.
            assert_eq!(request["query"], RECENT_PULLS_QUERY);
            let owner = request["variables"]["owner"].as_str().unwrap().to_owned();
            let name = request["variables"]["name"].as_str().unwrap().to_owned();
            let full = format!("{owner}/{name}");
            let answer = match name.as_str() {
                "sealmesh" => pulls_body(
                    &full,
                    r#"{"headRefName":"feature/xyz-62-archive","baseRefName":"main","state":"MERGED"},
                       {"headRefName":"feature/xyz-63-next","baseRefName":"feature/xyz-62-archive","state":"OPEN"},
                       {"headRefName":"feature/xyz-1-old","baseRefName":"main","state":"CLOSED"},
                       {"headRefName":"feature/xyz-64-part-2","baseRefName":"feature/xyz-64-part-1","state":"MERGED"}"#,
                ),
                "empty" => pulls_body(&full, ""),
                "renamed" => pulls_body("example-org/elsewhere", ""),
                "odd-state" => pulls_body(
                    &full,
                    r#"{"headRefName":"feature/xyz-62","baseRefName":"main","state":"DRAFT"}"#,
                ),
                "null-node" => pulls_body(&full, "null"),
                "no-default-branch" => pulls_body_on(&full, "null", ""),
                "default-branch-absent" => format!(
                    r#"{{"data":{{"repository":{{"nameWithOwner":"{full}","pullRequests":{{"nodes":[]}}}}}}}}"#
                ),
                "partial" => format!(
                    r#"{{"data":{{"repository":{{"nameWithOwner":"{full}","pullRequests":{{"nodes":[]}}}}}},"errors":[{{"message":"something"}}]}}"#
                ),
                "hidden" => {
                    r#"{"data":{"repository":null},"errors":[{"type":"NOT_FOUND"}]}"#.to_owned()
                }
                "rate-limited" => {
                    return (
                        StatusCode::FORBIDDEN,
                        r#"{"message":"rate limit"}"#.to_owned(),
                    );
                }
                "huge" => pulls_body(
                    &full,
                    &format!(
                        r#"{{"headRefName":"feature/xyz-62-{}","baseRefName":"main","state":"MERGED"}}"#,
                        "a".repeat(MAX_RESPONSE_BYTES)
                    ),
                ),
                _ => r#"{"data":{"repository":null}}"#.to_owned(),
            };
            (StatusCode::OK, answer)
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = axum::Router::new().route("/graphql", post(graphql));
        tokio::spawn(async move { axum::serve(listener, app).await });
        Github::with_api_root("example-org".to_owned(), format!("http://{addr}")).unwrap()
    }

    #[test]
    fn the_listing_asks_for_exactly_the_recent_pulls_it_is_capped_to() {
        assert!(RECENT_PULLS_QUERY.contains(&format!("pullRequests(first:{RECENT_PULLS},")));
        assert!(RECENT_PULLS_QUERY.contains("orderBy:{field:UPDATED_AT,direction:DESC}"));
        assert!(RECENT_PULLS_QUERY.contains("nodes{headRefName baseRefName state}"));
        assert!(RECENT_PULLS_QUERY.contains("defaultBranchRef{name}"));
    }

    #[tokio::test]
    async fn recent_pulls_come_back_with_their_branch_and_state() {
        let github = fake_graphql().await;
        let pulls = github
            .recent_pulls(&caller(), &repo("example-org/sealmesh"))
            .await
            .unwrap();
        assert_eq!(
            pulls,
            vec![
                PullSummary {
                    head_ref: "feature/xyz-62-archive".to_owned(),
                    into_default_branch: true,
                    state: PullState::Merged
                },
                PullSummary {
                    head_ref: "feature/xyz-63-next".to_owned(),
                    into_default_branch: false,
                    state: PullState::Open
                },
                PullSummary {
                    head_ref: "feature/xyz-1-old".to_owned(),
                    into_default_branch: true,
                    state: PullState::Closed
                },
                // Merged, but into another feature branch: a stacked part.
                PullSummary {
                    head_ref: "feature/xyz-64-part-2".to_owned(),
                    into_default_branch: false,
                    state: PullState::Merged
                },
            ]
        );
    }

    #[tokio::test]
    async fn a_repo_with_no_pull_requests_is_an_empty_list_not_a_failure() {
        let github = fake_graphql().await;
        assert_eq!(
            github
                .recent_pulls(&caller(), &repo("example-org/empty"))
                .await,
            Ok(Vec::new())
        );
    }

    #[tokio::test]
    async fn pulls_are_listed_with_the_callers_token_and_nothing_else() {
        let github = fake_graphql().await;
        assert_eq!(
            github
                .recent_pulls(
                    &SecretToken::new("ghp_someone_else".to_owned()),
                    &repo("example-org/sealmesh")
                )
                .await,
            Err(PullsUnverifiable)
        );
    }

    #[tokio::test]
    async fn a_listing_that_does_not_settle_the_question_is_unverifiable() {
        let github = fake_graphql().await;
        for slug in [
            "example-org/renamed",
            "example-org/odd-state",
            "example-org/null-node",
            "example-org/no-default-branch",
            "example-org/default-branch-absent",
            "example-org/partial",
            "example-org/hidden",
            "example-org/missing",
            "example-org/rate-limited",
        ] {
            assert_eq!(
                github.recent_pulls(&caller(), &repo(slug)).await,
                Err(PullsUnverifiable),
                "{slug}"
            );
        }
    }

    #[tokio::test]
    async fn an_oversized_listing_is_refused_before_it_is_parsed() {
        // Valid JSON reporting a merge, so a parser that ran first would
        // archive the room.
        let github = fake_graphql().await;
        assert_eq!(
            github
                .recent_pulls(&caller(), &repo("example-org/huge"))
                .await,
            Err(PullsUnverifiable)
        );
    }

    #[test]
    fn more_nodes_than_were_asked_for_is_unverifiable() {
        let node = r#"{"headRefName":"main","baseRefName":"main","state":"OPEN"}"#;
        let nodes = vec![node; RECENT_PULLS + 1].join(",");
        let body = pulls_body("example-org/sealmesh", &nodes);
        assert_eq!(
            pulls_from_body(&repo("example-org/sealmesh"), body.as_bytes()),
            Err(PullsUnverifiable)
        );
    }

    #[test]
    fn a_token_does_not_render_into_a_debug_dump() {
        let token = SecretToken::new("ghp_realsecretvalue".to_owned());
        assert!(!format!("{token:?}").contains("ghp_realsecretvalue"));
    }
}
