//! What `room_join`, `room_post` and `room_read` share: the caller's own
//! checkout, the daemon's local socket, and one closed set of failures.
//!
//! # The caller's own rooms, and no others (Q5)
//!
//! None of the three takes a room. Each reads the checkout the MCP server runs
//! in (`gitref::read`, from its working directory), asks the running daemon
//! which rooms that checkout belongs to (`resolve`), and acts on those.
//! "Own checkout" means the directory the server was launched in, with
//! whatever branch and `origin` it says: that picks which rooms are named,
//! and the router's access check on every join is the real boundary. A
//! tool that takes a room names one by its *scope* — `ticket`, `repo` or
//! `project` — which picks among the caller's own rooms and cannot name
//! anyone else's. No input names an agent, a host, a session, a room owner
//! or a drain consumer.
//!
//! # The daemon, not daemon code
//!
//! Rooms are reached only through the running daemon's local socket, with
//! `sealmesh::room::connect`. The daemon holds this machine's one router
//! connection that may join rooms, and its token never crosses the socket, so
//! nothing here touches a credential and nothing here uses
//! [`crate::router::RouterAccess`] for more than reading the config. That is
//! also why the room tools do not need `github_auth::token()` moved off the
//! runtime: none of them runs `gh`.
//!
//! # Project rooms are opt-in twice (Q3)
//!
//! A project room is only ever acted on when `[chat] project_rooms` is on in
//! this server's config, and the daemon only names one when it is on in the
//! daemon's. `room_join` additionally joins it only when asked with
//! `project: true`.
//!
//! # Failures are categories
//!
//! Every way a call can fail is a word in [`RoomFailure`], never a message:
//! the daemon's errors can carry a path, and a refusal must not say which
//! check failed (rules 14, 25, 26).

use std::{future::Future, path::PathBuf, time::Duration};

use rmcp::schemars::JsonSchema;
use sealmesh::{
    gitref::{self, Checkout},
    room::{
        connect::{self, ConnectError, DEFAULT_DEADLINE},
        local::{BackfillCategory, PostCategory, Request, Response},
    },
};
use sealmesh_chat_wire::{RoomId, Scope};
use serde::{Deserialize, Serialize};

use crate::router::RouterAccess;

/// The longest a call may take up to the request that acts on the room: the
/// resolve, and for `room_join` each join. Longer than one request's
/// [`DEFAULT_DEADLINE`], so a single slow request reads as the daemon's own
/// answer, and short enough that a join of three rooms against a daemon that
/// has stopped answering does not hold an agent's turn for over a minute.
/// `room_post`'s post is not under it: see [`Context::call_deadline`].
pub const CALL_DEADLINE: Duration = Duration::from_secs(30);

const _: () = assert!(
    CALL_DEADLINE.as_nanos() > DEFAULT_DEADLINE.as_nanos(),
    "a room tool call must outlast one daemon request"
);

/// Which of the caller's own rooms: the room of the ticket its branch names,
/// the room of its repo, or its project's room.
///
/// A scope, not a room. It is resolved against the checkout this server runs
/// in, so it can only ever pick one of the caller's own rooms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(rename_all = "snake_case")]
pub enum RoomScope {
    Ticket,
    Repo,
    Project,
}

impl RoomScope {
    pub fn of(room: &RoomId) -> Self {
        match room.scope {
            Scope::Ticket(_) => Self::Ticket,
            Scope::Repo(_) => Self::Repo,
            Scope::Project => Self::Project,
        }
    }
}

/// Why a room tool call did nothing. A closed set; see the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "error", rename_all = "snake_case")]
pub enum RoomFailure {
    /// A call to this tool in this process went through too recently. Nothing
    /// was sent to the daemon.
    RateLimited,
    /// The sealmesh config could not be read.
    ConfigUnavailable,
    /// The working directory is not a checkout whose `origin` is on GitHub.
    NotACheckout,
    /// This checkout belongs to no room of that scope. One answer for a repo
    /// in no project, one this machine cannot read, and a project room that
    /// is not turned on (rule 25).
    NoRoom,
    /// The body was empty or over the cap. Refused by the chat wire's own
    /// constructor, never truncated. Nothing was sent.
    BodyRefused,
    /// No daemon is listening on this user's socket.
    DaemonNotRunning,
    /// The socket's directory is reachable by other users, so the daemon
    /// behind it is not trusted. Restarting the daemon narrows it.
    DaemonUntrusted,
    /// The daemon closed the connection unanswered: it is at its limit of
    /// local connections.
    DaemonRefused,
    /// The daemon's answer could not be read, or was not an answer to what
    /// was asked. It may be running an older build.
    DaemonError,
    /// Chat is off on this machine: `[chat] enabled` is false, or the kill
    /// file exists.
    Disabled,
    /// The daemon has no router connection right now.
    NotConnected,
    /// The daemon has too much in flight. Ask again shortly.
    Busy,
    /// The daemon, or the router behind it, did not answer in time.
    TimedOut,
    /// This machine has not joined that room. `room_join` first.
    NotMember,
    /// The router refused the post. `category` is `rate_limited`,
    /// `budget_exhausted`, `turn_capped`, `not_recorded`, `archived`, or
    /// `unknown` for a category this build cannot name.
    PostRefused { category: PostCategory },
    /// The router read nothing. `category` is `not_held`, `busy`,
    /// `unavailable`, or `unknown`.
    BackfillRefused { category: BackfillCategory },
    /// The result was over the output budget, so it was not sent.
    OutputTooLarge,
}

/// The failure a daemon answer stands for, when it is not the answer the
/// caller was waiting for.
pub fn refusal(response: Response) -> RoomFailure {
    match response {
        Response::Disabled {} => RoomFailure::Disabled,
        Response::NotConnected {} => RoomFailure::NotConnected,
        Response::Busy {} => RoomFailure::Busy,
        Response::TimedOut {} => RoomFailure::TimedOut,
        Response::NotMember { .. } => RoomFailure::NotMember,
        Response::PostRefused { category, .. } => RoomFailure::PostRefused { category },
        Response::BackfillRefused { category, .. } => RoomFailure::BackfillRefused { category },
        Response::Invalid {}
        | Response::TooLarge {}
        | Response::Status { .. }
        | Response::Joined { .. }
        | Response::JoinRefused { .. }
        | Response::RoomLimit {}
        | Response::Left { .. }
        | Response::Posted { .. }
        | Response::PostUnconfirmed { .. }
        | Response::Backfill { .. }
        | Response::Messages { .. }
        | Response::Tail { .. }
        | Response::Rooms { .. }
        | Response::RoomListRefused {}
        | Response::ListThrottled { .. }
        | Response::Resolved { .. } => RoomFailure::DaemonError,
    }
}

/// The failure a connection error stands for.
pub(crate) fn unreachable(error: &ConnectError) -> RoomFailure {
    match error {
        ConnectError::NoHome | ConnectError::NotRunning(_) => RoomFailure::DaemonNotRunning,
        ConnectError::Unsafe(_) => RoomFailure::DaemonUntrusted,
        ConnectError::Refused => RoomFailure::DaemonRefused,
        ConnectError::TimedOut(_) => RoomFailure::TimedOut,
        ConnectError::TooLarge | ConnectError::Protocol { .. } | ConnectError::Io(_) => {
            RoomFailure::DaemonError
        }
    }
}

/// Whether a request that failed this way may still have reached the daemon.
///
/// Only a daemon that is not there, or a socket that is not trusted, is
/// certain never to have read it: both are known before anything is
/// written. Every other failure — a deadline, a reset, an answer that could
/// not be read — can come after the daemon read the request and acted on it.
/// For a post, that is the difference between "safe to retry" and "a retry
/// may duplicate it".
pub(crate) fn may_have_arrived(error: &ConnectError) -> bool {
    match error {
        ConnectError::NoHome | ConnectError::NotRunning(_) | ConnectError::Unsafe(_) => false,
        ConnectError::Refused
        | ConnectError::TimedOut(_)
        | ConnectError::TooLarge
        | ConnectError::Protocol { .. }
        | ConnectError::Io(_) => true,
    }
}

/// Everything a room tool call reads from where it runs.
#[derive(Debug, Clone)]
pub struct Context {
    /// The directory whose checkout is the caller's: the server's working
    /// directory, which a harness sets to the agent's own.
    pub dir: PathBuf,
    /// Ticket prefixes to read from a branch name, from the config.
    pub prefixes: Vec<String>,
    /// `[chat] project_rooms` in this server's config.
    pub project_rooms: bool,
    /// The daemon's socket. `None` when there is no home to find it under.
    pub socket: Option<PathBuf>,
    /// How long one request to the daemon may take.
    pub request_deadline: Duration,
    /// How long a call may take before the request that acts: the resolve,
    /// and for `room_join` the joins too. `room_post`'s post is outside it,
    /// bounded by its own `request_deadline`, so this deadline can never
    /// expire on a post that was already written.
    pub call_deadline: Duration,
    /// How long `room_post` waits for its drain key before posting without.
    pub lookup_deadline: Duration,
}

impl Context {
    /// The context of this process: its config, its working directory and
    /// the daemon's usual socket.
    pub fn from_process(router: &RouterAccess) -> Result<Self, RoomFailure> {
        let config = router
            .load_config()
            .map_err(|_| RoomFailure::ConfigUnavailable)?;
        let dir = std::env::current_dir().map_err(|_| RoomFailure::NotACheckout)?;
        Ok(Self {
            dir,
            prefixes: config.ticket_prefixes,
            project_rooms: config.chat.project_rooms,
            socket: daemon_socket(),
            request_deadline: DEFAULT_DEADLINE,
            call_deadline: CALL_DEADLINE,
            lookup_deadline: crate::session::LOOKUP_DEADLINE,
        })
    }

    /// The caller's own checkout.
    pub fn checkout(&self) -> Result<Checkout, RoomFailure> {
        gitref::read(&self.dir, &self.prefixes).ok_or(RoomFailure::NotACheckout)
    }

    /// One request to the daemon, one answer.
    pub async fn ask(&self, request: &Request) -> Result<Response, RoomFailure> {
        self.send(request)
            .await
            .map_err(|error| unreachable(&error))
    }

    /// [`Self::ask`], keeping the connection error, so a caller that must know
    /// whether the request may have arrived can ask [`may_have_arrived`].
    pub async fn send(&self, request: &Request) -> Result<Response, ConnectError> {
        let Some(socket) = &self.socket else {
            return Err(ConnectError::NoHome);
        };
        connect::request(socket, request, self.request_deadline).await
    }

    /// The rooms `checkout` belongs to, as the daemon resolves them from the
    /// router's project map. A project room is dropped unless
    /// `[chat] project_rooms` is on here too. Joins nothing.
    pub async fn resolve(&self, checkout: &Checkout) -> Result<Vec<RoomId>, RoomFailure> {
        let request = Request::Resolve {
            repo: checkout.repo.clone(),
            ticket: checkout.ticket.clone(),
            join: false,
        };
        match self.ask(&request).await? {
            Response::Resolved { rooms } => Ok(rooms
                .into_iter()
                .map(|resolved| resolved.room)
                .filter(|room| self.project_rooms || RoomScope::of(room) != RoomScope::Project)
                .collect()),
            other => Err(refusal(other)),
        }
    }

    /// The caller's own room of `scope`.
    pub async fn room(&self, checkout: &Checkout, scope: RoomScope) -> Result<RoomId, RoomFailure> {
        self.resolve(checkout)
            .await?
            .into_iter()
            .find(|room| RoomScope::of(room) == scope)
            .ok_or(RoomFailure::NoRoom)
    }
}

/// Where the running daemon listens.
#[cfg(not(test))]
fn daemon_socket() -> Option<PathBuf> {
    sealmesh::room::socket::default_path()
}

/// Under test, no call through [`Context::from_process`] can reach this
/// machine's real daemon, whatever the rest of the call does. A test that
/// needs the socket builds a [`Context`] with its own stand-in.
#[cfg(test)]
fn daemon_socket() -> Option<PathBuf> {
    None
}

/// `step`, or [`RoomFailure::TimedOut`] once `limit` has passed.
pub async fn bounded<T>(
    limit: Duration,
    step: impl Future<Output = Result<T, RoomFailure>>,
) -> Result<T, RoomFailure> {
    tokio::time::timeout(limit, step)
        .await
        .unwrap_or(Err(RoomFailure::TimedOut))
}

/// A stand-in daemon and a checkout, for the three tools' tests.
#[cfg(test)]
pub(crate) mod fake {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use std::{
        os::unix::fs::DirBuilderExt as _,
        path::Path,
        sync::{Arc, Mutex},
    };

    use sealmesh::room::local::{self, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, ResolvedRoom};
    use sealmesh_chat_wire::{RoomId, Scope};
    use sealmesh_wire::{ProjectId, RepoSlug, TicketId};
    use tokio::{
        io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader},
        net::UnixListener,
    };

    use super::*;

    pub(crate) fn example_repo(scope: Scope) -> RoomId {
        RoomId::new(ProjectId::try_from("example-repo").unwrap(), scope)
    }

    pub(crate) fn ticket_room() -> RoomId {
        example_repo(Scope::Ticket(TicketId::new("ENG", 7).unwrap()))
    }

    pub(crate) fn repo_room() -> RoomId {
        example_repo(Scope::Repo(
            RepoSlug::try_from("example-org/example-repo").unwrap(),
        ))
    }

    pub(crate) fn project_room() -> RoomId {
        example_repo(Scope::Project)
    }

    /// A daemon's `resolve` answer naming `rooms`, joined or not.
    pub(crate) fn resolved(rooms: &[RoomId]) -> Response {
        Response::Resolved {
            rooms: rooms
                .iter()
                .map(|room| ResolvedRoom {
                    room: room.clone(),
                    join: None,
                })
                .collect(),
        }
    }

    type Answer = Arc<dyn Fn(&Request) -> Option<Response> + Send + Sync>;

    /// A stand-in for the daemon's socket server: it reads each request line,
    /// records it, and answers what `answer` says. `None` holds the
    /// connection open unanswered.
    pub(crate) struct FakeDaemon {
        pub(crate) root: PathBuf,
        pub(crate) socket: PathBuf,
        requests: Arc<Mutex<Vec<Request>>>,
        task: tokio::task::JoinHandle<()>,
        _checkout: Option<GitCheckout>,
    }

    impl Drop for FakeDaemon {
        fn drop(&mut self) {
            self.task.abort();
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    impl FakeDaemon {
        pub(crate) fn start(
            tag: &str,
            answer: impl Fn(&Request) -> Option<Response> + Send + Sync + 'static,
        ) -> Self {
            Self::start_slow(tag, |_| Duration::ZERO, answer)
        }

        /// [`Self::start`], waiting `delay(request)` before each answer.
        pub(crate) fn start_slow(
            tag: &str,
            delay: impl Fn(&Request) -> Duration + Send + Sync + 'static,
            answer: impl Fn(&Request) -> Option<Response> + Send + Sync + 'static,
        ) -> Self {
            let delay: Arc<dyn Fn(&Request) -> Duration + Send + Sync> = Arc::new(delay);
            // Short, because a Unix socket path is capped near 104 bytes.
            let root =
                std::env::temp_dir().join(format!("awm{}{tag}", std::process::id() % 100_000));
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(&root).unwrap();
            let dir = root.join("local");
            std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
            let socket = dir.join("daemon.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let requests = Arc::new(Mutex::new(Vec::new()));
            let answer: Answer = Arc::new(answer);
            let seen = requests.clone();
            let task = tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let seen = seen.clone();
                    let answer = answer.clone();
                    let delay = delay.clone();
                    tokio::spawn(async move {
                        let (read, mut write) = stream.into_split();
                        let mut line = String::new();
                        BufReader::new(read).read_line(&mut line).await.unwrap();
                        let request: Request =
                            local::decode(line.trim_end().as_bytes(), MAX_REQUEST_BYTES).unwrap();
                        let reply = answer(&request);
                        let wait = delay(&request);
                        seen.lock().unwrap().push(request);
                        tokio::time::sleep(wait).await;
                        match reply {
                            Some(response) => {
                                let bytes = local::encode(&response, MAX_RESPONSE_BYTES).unwrap();
                                let _ = write.write_all(&bytes).await;
                            }
                            None => std::future::pending::<()>().await,
                        }
                    });
                }
            });
            Self {
                root,
                socket,
                requests,
                task,
                _checkout: None,
            }
        }

        pub(crate) fn requests(&self) -> Vec<Request> {
            self.requests.lock().unwrap().clone()
        }

        /// A context on a checkout of `example-org/example-repo` on branch
        /// `feature/eng-7-portal`, talking to this daemon.
        pub(crate) fn context(&mut self, project_rooms: bool) -> Context {
            let checkout = GitCheckout::new(&self.root.join("repo"), "feature/eng-7-portal");
            let dir = checkout.0.clone();
            self._checkout = Some(checkout);
            Context {
                dir,
                prefixes: vec!["ENG".to_owned()],
                project_rooms,
                socket: Some(self.socket.clone()),
                request_deadline: Duration::from_secs(5),
                call_deadline: Duration::from_secs(10),
                lookup_deadline: Duration::from_secs(2),
            }
        }
    }

    /// A git checkout with an `origin` on GitHub, removed with its daemon.
    pub(crate) struct GitCheckout(pub(crate) PathBuf);

    impl GitCheckout {
        pub(crate) fn new(dir: &Path, branch: &str) -> Self {
            std::fs::create_dir_all(dir).unwrap();
            let git = |args: &[&str]| {
                let out = std::process::Command::new("git")
                    .args(args)
                    .current_dir(dir)
                    .env("GIT_AUTHOR_NAME", "t")
                    .env("GIT_AUTHOR_EMAIL", "t@example.test")
                    .env("GIT_COMMITTER_NAME", "t")
                    .env("GIT_COMMITTER_EMAIL", "t@example.test")
                    .env("GIT_CONFIG_NOSYSTEM", "1")
                    .output()
                    .unwrap();
                assert!(out.status.success(), "git {args:?} failed: {out:?}");
            };
            git(&["init", &format!("--initial-branch={branch}"), "."]);
            std::fs::write(dir.join("a.txt"), "one").unwrap();
            git(&["add", "."]);
            git(&["commit", "-m", "one"]);
            git(&[
                "remote",
                "add",
                "origin",
                "git@github.com:example-org/example-repo.git",
            ]);
            Self(dir.to_path_buf())
        }
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

    use sealmesh::room::local::Category;
    use sealmesh_proto::{BackfillRefusal, PostRefusal};
    use serde_json::json;

    use super::{fake::*, *};

    #[test]
    fn a_scope_word_is_one_of_three_and_nothing_else() {
        for (word, scope) in [
            ("ticket", RoomScope::Ticket),
            ("repo", RoomScope::Repo),
            ("project", RoomScope::Project),
        ] {
            assert_eq!(
                serde_json::from_value::<RoomScope>(json!(word)).unwrap(),
                scope
            );
        }
        for hostile in [
            json!("ENG-7"),
            json!("example-org/example-repo"),
            json!({ "project": "example-repo", "scope": "project" }),
            json!("Ticket"),
        ] {
            assert!(
                serde_json::from_value::<RoomScope>(hostile.clone()).is_err(),
                "{hostile}"
            );
        }
        assert_eq!(RoomScope::of(&ticket_room()), RoomScope::Ticket);
        assert_eq!(RoomScope::of(&repo_room()), RoomScope::Repo);
        assert_eq!(RoomScope::of(&project_room()), RoomScope::Project);
    }

    #[test]
    fn every_failure_serialises_as_a_category_and_nothing_else() {
        for (failure, expected) in [
            (RoomFailure::RateLimited, json!({ "error": "rate_limited" })),
            (
                RoomFailure::ConfigUnavailable,
                json!({ "error": "config_unavailable" }),
            ),
            (
                RoomFailure::NotACheckout,
                json!({ "error": "not_a_checkout" }),
            ),
            (RoomFailure::NoRoom, json!({ "error": "no_room" })),
            (RoomFailure::BodyRefused, json!({ "error": "body_refused" })),
            (
                RoomFailure::DaemonNotRunning,
                json!({ "error": "daemon_not_running" }),
            ),
            (
                RoomFailure::DaemonUntrusted,
                json!({ "error": "daemon_untrusted" }),
            ),
            (
                RoomFailure::DaemonRefused,
                json!({ "error": "daemon_refused" }),
            ),
            (RoomFailure::DaemonError, json!({ "error": "daemon_error" })),
            (RoomFailure::Disabled, json!({ "error": "disabled" })),
            (
                RoomFailure::NotConnected,
                json!({ "error": "not_connected" }),
            ),
            (RoomFailure::Busy, json!({ "error": "busy" })),
            (RoomFailure::TimedOut, json!({ "error": "timed_out" })),
            (RoomFailure::NotMember, json!({ "error": "not_member" })),
            (
                RoomFailure::PostRefused {
                    category: Category::Known(PostRefusal::BudgetExhausted),
                },
                json!({ "error": "post_refused", "category": "budget_exhausted" }),
            ),
            (
                RoomFailure::PostRefused {
                    category: Category::UNKNOWN,
                },
                json!({ "error": "post_refused", "category": "unknown" }),
            ),
            (
                RoomFailure::BackfillRefused {
                    category: Category::Known(BackfillRefusal::NotHeld),
                },
                json!({ "error": "backfill_refused", "category": "not_held" }),
            ),
            (
                RoomFailure::OutputTooLarge,
                json!({ "error": "output_too_large" }),
            ),
        ] {
            assert_eq!(serde_json::to_value(failure).unwrap(), expected);
        }
    }

    #[test]
    fn every_daemon_answer_that_is_a_refusal_keeps_its_own_category() {
        let room = repo_room;
        for (response, failure) in [
            (Response::Disabled {}, RoomFailure::Disabled),
            (Response::NotConnected {}, RoomFailure::NotConnected),
            (Response::Busy {}, RoomFailure::Busy),
            (Response::TimedOut {}, RoomFailure::TimedOut),
            (Response::NotMember { room: room() }, RoomFailure::NotMember),
            (Response::Invalid {}, RoomFailure::DaemonError),
            (Response::TooLarge {}, RoomFailure::DaemonError),
            (Response::Joined { room: room() }, RoomFailure::DaemonError),
        ] {
            assert_eq!(refusal(response), failure);
        }
        for category in [
            PostRefusal::RateLimited,
            PostRefusal::BudgetExhausted,
            PostRefusal::TurnCapped,
            PostRefusal::NotRecorded,
            PostRefusal::Archived,
        ]
        .map(Category::Known)
        .into_iter()
        .chain([Category::UNKNOWN])
        {
            assert_eq!(
                refusal(Response::PostRefused {
                    room: room(),
                    category
                }),
                RoomFailure::PostRefused { category }
            );
        }
    }

    #[test]
    fn only_a_daemon_that_is_not_there_certainly_never_read_the_request() {
        let path = PathBuf::from("/nonexistent");
        for (error, arrived) in [
            (ConnectError::NoHome, false),
            (ConnectError::NotRunning(path.clone()), false),
            (ConnectError::Unsafe(path), false),
            (ConnectError::Refused, true),
            (ConnectError::TimedOut(Duration::from_secs(1)), true),
            (ConnectError::TooLarge, true),
            (ConnectError::Protocol { line: 1, column: 1 }, true),
            (ConnectError::Io(std::io::Error::other("reset")), true),
        ] {
            assert_eq!(may_have_arrived(&error), arrived, "{error}");
        }
    }

    #[tokio::test]
    async fn nothing_listening_is_daemon_not_running_at_once() {
        let mut daemon = FakeDaemon::start("nl", |_| None);
        let mut context = daemon.context(false);
        context.socket = Some(daemon.root.join("local").join("nobody.sock"));
        let started = std::time::Instant::now();
        let checkout = context.checkout().unwrap();
        assert_eq!(
            context.resolve(&checkout).await,
            Err(RoomFailure::DaemonNotRunning)
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        context.socket = None;
        assert_eq!(
            context.resolve(&checkout).await,
            Err(RoomFailure::DaemonNotRunning)
        );
    }

    #[tokio::test]
    async fn a_socket_directory_others_can_reach_is_daemon_untrusted() {
        use std::os::unix::fs::PermissionsExt as _;
        let mut daemon = FakeDaemon::start("ut", |_| Some(resolved(&[repo_room()])));
        let context = daemon.context(false);
        std::fs::set_permissions(
            daemon.socket.parent().unwrap(),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        let checkout = context.checkout().unwrap();
        assert_eq!(
            context.resolve(&checkout).await,
            Err(RoomFailure::DaemonUntrusted)
        );
        assert!(daemon.requests().is_empty());
    }

    #[tokio::test]
    async fn a_daemon_that_never_answers_is_timed_out() {
        let mut daemon = FakeDaemon::start("to", |_| None);
        let mut context = daemon.context(false);
        context.request_deadline = Duration::from_millis(200);
        let checkout = context.checkout().unwrap();
        assert_eq!(context.resolve(&checkout).await, Err(RoomFailure::TimedOut));
    }

    #[tokio::test]
    async fn a_project_room_is_dropped_unless_project_rooms_is_on_here() {
        let mut daemon = FakeDaemon::start("pr", |_| {
            Some(resolved(&[ticket_room(), repo_room(), project_room()]))
        });
        let off = daemon.context(false);
        let checkout = off.checkout().unwrap();
        assert_eq!(
            off.resolve(&checkout).await.unwrap(),
            vec![ticket_room(), repo_room()]
        );
        let on = Context {
            project_rooms: true,
            ..off
        };
        assert_eq!(
            on.resolve(&checkout).await.unwrap(),
            vec![ticket_room(), repo_room(), project_room()]
        );
    }

    #[test]
    fn a_directory_that_is_no_checkout_is_not_a_checkout() {
        let dir = std::env::temp_dir().join(format!("awm-nocheckout-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let context = Context {
            dir: dir.clone(),
            prefixes: vec![],
            project_rooms: false,
            socket: None,
            request_deadline: DEFAULT_DEADLINE,
            call_deadline: CALL_DEADLINE,
            lookup_deadline: crate::session::LOOKUP_DEADLINE,
        };
        assert_eq!(context.checkout().unwrap_err(), RoomFailure::NotACheckout);
        let _ = std::fs::remove_dir_all(dir);
    }
}
