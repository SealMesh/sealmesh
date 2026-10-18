//! `fleet_status`: what `sealmesh who` shows, for an agent rather than a
//! person at a terminal.
//!
//! # One reader path
//!
//! It opens a reader connection through [`RouterAccess`] and hands the
//! welcomed socket to [`who::ask`], the same function `who` runs. So it
//! inherits, rather than re-implements, everything that path holds:
//!
//! - **The repo-access gate.** The router narrows every reader's answer to
//!   repos the reader's own token can read, before it leaves the router. There
//!   is no second route to a snapshot here for that check to be missed on
//!   (rule 5).
//! - **The partial bit.** A `ServerFrame::Partial` sets [`FleetStatus::partial`],
//!   a field rather than a sentence, so an agent can tell a narrowed answer from
//!   a quiet fleet.
//! - **Attribution (rule 21).** `who` refuses a snapshot whose own `host`
//!   disagrees with the router's attribution. That row arrives here as
//!   [`HostEntry::Unreadable`], not as the snapshot.
//!
//! # No prose
//!
//! Every field is a wire or proto type, a number, a bool, or a closed enum.
//! `who`'s reason string on an unreadable row is dropped rather than passed on,
//! because it is a sentence; the state says the row was refused.
//!
//! # No target
//!
//! It takes no arguments. It reports the fleet and addresses nobody (Q5).
//!
//! # Cooldown (rule 24)
//!
//! Every call is a full fan-out, and each daemon answers from one shared
//! budget. An agent calling in a loop would drain it, and every other reader
//! would then see those machines refusing. So calls less than [`COOLDOWN`]
//! apart are refused with [`FleetStatusFailure::RateLimited`] before any
//! connection is opened. The cooldown is one static for the whole process,
//! because each call gets a fresh `RouterAccess` and anything held on it would
//! reset every time the agent called again.

use std::{
    future::Future,
    ops::ControlFlow,
    time::{Duration, Instant},
};

use rmcp::{model::ToolAnnotations, schemars::JsonSchema};
use sealmesh::{
    client::{Connected, Socket},
    config::Config,
    daemon::QUERY_REFILL,
    progress::Phase,
    who::{self, Answers, HostReport},
};
use sealmesh_proto::ProjectEntry;
use sealmesh_wire::{HostId, Snapshot};
use serde::{Deserialize, Serialize};

use super::{McpTool, cooldown::Cooldown};
use crate::router::RouterAccess;

/// The most machines one answer may report before it is refused. An answer is
/// held in memory until the router says it is complete, and the router is a
/// service that can itself be compromised, so the count is bounded here — at
/// the reader — rather than trusted.
pub const MAX_HOSTS: usize = 256;

/// How long connecting and the whole query may take. Well above the router's
/// own fan-out deadline, so an honest router always answers inside it; a
/// router that never sends `Complete` does not hold an agent's tool call open.
pub const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// The least time between two `fleet_status` calls that may reach the router
/// from one process.
///
/// A daemon earns back one query every [`QUERY_REFILL`] (6s). At 30s, one
/// looping agent spends a fifth of that, so a person running `who` every ten
/// seconds alongside it is never refused, and even four looping processes on
/// their own stay below the refill rate.
pub const COOLDOWN: Duration = Duration::from_secs(30);

/// The least [`COOLDOWN`] may ever be: twice a daemon's refill interval, so one
/// looping process can never spend more than half of what every daemon earns
/// back. Checked at compile time below.
pub const COOLDOWN_FLOOR: Duration = QUERY_REFILL.saturating_mul(2);

const _: () = assert!(
    COOLDOWN.as_nanos() >= COOLDOWN_FLOOR.as_nanos(),
    "fleet_status's cooldown is below twice a daemon's query refill"
);

/// The cooldown every `fleet_status` call in this process shares.
static GATE: Cooldown = Cooldown::new(COOLDOWN);

pub struct FleetStatusTool;

/// `fleet_status` takes nothing.
#[derive(Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub struct FleetStatusArgs {}

/// The fleet, as this login may see it.
#[derive(Debug, Serialize)]
pub struct FleetStatus {
    /// The router withheld work in repos this login cannot read. One bit for
    /// the whole answer: which machine, which repo and how much are exactly
    /// what the reader may not learn (rule 25).
    pub partial: bool,
    /// Answers this build could not decode at all. Their machines cannot be
    /// named, so they cannot be rows, and dropping the count would make a
    /// short answer look complete.
    pub undecodable: usize,
    /// One entry per machine, in the order the router reported them.
    pub hosts: Vec<HostEntry>,
    /// The repo→project mapping, already filtered by the router to repos this
    /// login can read. Empty when the router has none.
    pub projects: Vec<ProjectEntry>,
}

/// One machine. `state` keeps apart the cases that must not collapse: a
/// closed laptop is not a deliberate opt-out, and neither is a refused answer.
#[derive(Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum HostEntry {
    /// It answered, and the answer is its own.
    Answering {
        host: HostId,
        snapshot: Box<Snapshot>,
    },
    /// Connected, and its owner has turned sharing off.
    NotSharing { host: HostId },
    /// No daemon attached, or it did not answer in time.
    Unreachable { host: HostId },
    /// Connected and sharing, but it refused this query because it has been
    /// asked too often. Not a closed laptop: it will answer again shortly.
    Throttled { host: HostId },
    /// It answered, and the answer was refused — today, because it claimed to
    /// be a different machine from the one the router attributed it to.
    Unreadable { host: HostId },
}

/// Why a call produced no fleet. A category, never a message: the underlying
/// error can carry a URL or a peer's bytes, and neither belongs in a tool
/// result (rules 14, 26).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "error", rename_all = "snake_case")]
pub enum FleetStatusFailure {
    /// The sealmesh config could not be read.
    ConfigUnavailable,
    /// The router could not be reached, `gh` gave no credential, or the
    /// router refused this login.
    RouterUnavailable,
    /// The connection failed or was refused partway through the query.
    QueryFailed,
    /// More than [`MAX_HOSTS`] machines answered.
    TooManyHosts,
    /// Connecting and the query took longer than [`QUERY_TIMEOUT`].
    TimedOut,
    /// A call in this process went through less than [`COOLDOWN`] ago. Nothing
    /// was sent to the router.
    RateLimited,
    /// The answer was over [`super::MAX_OUTPUT_BYTES`] once encoded as it is
    /// sent, so it was not sent.
    OutputTooLarge,
}

impl McpTool for FleetStatusTool {
    const NAME: &'static str = "fleet_status";
    const DESCRIPTION: &'static str = "Every machine attached to the sealmesh router and the coding \
         agents running on each, as `sealmesh who` shows them, limited to repos your GitHub login \
         can read. Each host's `state` is answering, not_sharing, unreachable, throttled or \
         unreadable. A throttled machine is up but refused because it was asked too often. \
         `partial` is true when the router left out work you cannot read. Takes no arguments. \
         Calls less than 30 seconds apart fail with the error rate_limited and are not sent. \
         An answer too large to send fails with output_too_large.";

    type Args = FleetStatusArgs;
    type Output = FleetStatus;
    type Failure = FleetStatusFailure;

    fn annotations() -> ToolAnnotations {
        ToolAnnotations::with_title("Fleet status")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(true)
    }

    fn output_too_large() -> FleetStatusFailure {
        FleetStatusFailure::OutputTooLarge
    }

    async fn run(
        router: RouterAccess,
        FleetStatusArgs {}: FleetStatusArgs,
    ) -> Result<FleetStatus, FleetStatusFailure> {
        let config = router
            .load_config()
            .map_err(|_| FleetStatusFailure::ConfigUnavailable)?;
        // A config that cannot be read reaches no router, so it does not spend
        // the cooldown.
        reach(&GATE, Instant::now(), || open_reader(&config)).await
    }

    #[cfg(test)]
    fn complete_sample() -> serde_json::Value {
        serde_json::json!({})
    }
}

/// How [`FleetStatusTool::run`] opens its reader connection: the CLI's
/// credential path and the reader handshake.
#[cfg(not(test))]
async fn open_reader(config: &Config) -> anyhow::Result<Connected> {
    RouterAccess::connect_reader_with(config).await
}

/// Under test, a call that gets past the cooldown fails here at once. So no
/// test that reaches `run` can run `gh` or dial a router, whatever the cooldown
/// does. A test that needs the query path drives [`reach`] or [`query`] with
/// its own stand-in router instead.
#[cfg(test)]
async fn open_reader(_config: &Config) -> anyhow::Result<Connected> {
    anyhow::bail!("a test build never opens a real reader connection")
}

/// Everything from the cooldown on. `connect` is the only way a connection is
/// opened, and it is not called for a call the cooldown refuses.
async fn reach<C, F>(
    gate: &Cooldown,
    now: Instant,
    connect: C,
) -> Result<FleetStatus, FleetStatusFailure>
where
    C: FnOnce() -> F,
    F: Future<Output = anyhow::Result<Connected>>,
{
    if !gate.claim(now) {
        return Err(FleetStatusFailure::RateLimited);
    }
    bounded(QUERY_TIMEOUT, async {
        let Connected { socket, host: _ } = connect()
            .await
            .map_err(|_| FleetStatusFailure::RouterUnavailable)?;
        query(socket).await
    })
    .await
}

async fn bounded<T>(
    limit: Duration,
    step: impl Future<Output = Result<T, FleetStatusFailure>>,
) -> Result<T, FleetStatusFailure> {
    tokio::time::timeout(limit, step)
        .await
        .unwrap_or(Err(FleetStatusFailure::TimedOut))
}

/// Everything after the handshake: `who`'s own query, held to [`MAX_HOSTS`].
async fn query(socket: Socket) -> Result<FleetStatus, FleetStatusFailure> {
    let mut over_cap = false;
    let asked = who::ask(socket, |phase| match phase {
        Phase::Asking { answered } if answered > MAX_HOSTS => {
            over_cap = true;
            ControlFlow::Break(())
        }
        Phase::Connecting | Phase::Asking { .. } | Phase::Resolving { .. } => {
            ControlFlow::Continue(())
        }
    })
    .await;
    match asked {
        Ok(answers) if answers.reports.len() <= MAX_HOSTS => Ok(FleetStatus::from(answers)),
        Ok(_) => Err(FleetStatusFailure::TooManyHosts),
        Err(_) if over_cap => Err(FleetStatusFailure::TooManyHosts),
        Err(_) => Err(FleetStatusFailure::QueryFailed),
    }
}

impl From<Answers> for FleetStatus {
    fn from(answers: Answers) -> Self {
        let Answers {
            reports,
            mut undecodable,
            projects,
            partial,
        } = answers;
        let mut hosts = Vec::with_capacity(reports.len());
        for report in reports {
            match HostEntry::from_report(report) {
                Some(entry) => hosts.push(entry),
                // `who` holds a non-answering host as the string of a `HostId`
                // it already validated, so this does not happen. If it ever
                // did, the row is counted rather than dropped.
                None => undecodable = undecodable.saturating_add(1),
            }
        }
        Self {
            partial,
            undecodable,
            hosts,
            projects,
        }
    }
}

impl HostEntry {
    fn from_report(report: HostReport) -> Option<Self> {
        let host = |name: &str| HostId::try_from(name).ok();
        Some(match report {
            HostReport::Answering(snapshot) => Self::Answering {
                host: snapshot.host.clone(),
                snapshot,
            },
            HostReport::NotSharing { host: name } => Self::NotSharing { host: host(&name)? },
            HostReport::Unreachable { host: name } => Self::Unreachable { host: host(&name)? },
            HostReport::Throttled { host: name } => Self::Throttled { host: host(&name)? },
            HostReport::Unreadable {
                host: name,
                reason: _,
            } => Self::Unreadable { host: host(&name)? },
        })
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

    use futures_util::{SinkExt as _, StreamExt as _};
    use sealmesh_proto::{HostState, QueryId, ServerFrame};
    use sealmesh_wire::{ProjectId, RepoSlug};
    use serde_json::{Value, json};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{connect_async_with_config, tungstenite::Message};

    use super::*;
    use crate::tools::{fits, tool_for};

    fn host(name: &str) -> HostId {
        HostId::try_from(name).unwrap()
    }

    fn snapshot(host: &str) -> Box<Snapshot> {
        Box::new(
            Snapshot::from_json(
                format!(
                    r#"{{"schema":2,"host":"{host}","collected_at":"2026-08-10T19:56:51Z",
                         "agents":[{{"id":"a1b2c3d4","repo":"example-org/sealmesh",
                                     "run_state":"working","branch":"feature/x","pr":7,
                                     "tasks":{{"completed":1,"total":3}},
                                     "started_at":"2026-08-10T19:00:00Z",
                                     "updated_at":"2026-08-10T19:56:00Z"}}]}}"#
                )
                .as_bytes(),
            )
            .unwrap(),
        )
    }

    fn report(host_name: &str, state: HostState) -> ServerFrame {
        ServerFrame::Report {
            id: QueryId(1),
            host: host(host_name),
            state,
        }
    }

    /// A router that answers the projects request and one query with
    /// `frames`, then holds the connection open. Returns the reader's socket.
    async fn fake_router(projects: Vec<ProjectEntry>, frames: Vec<ServerFrame>) -> Socket {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let send =
                |frame: &ServerFrame| Message::Text(serde_json::to_string(frame).unwrap().into());
            // The projects request, then the ask.
            let _ = ws.next().await;
            ws.send(send(&ServerFrame::Projects { projects }))
                .await
                .unwrap();
            let _ = ws.next().await;
            for frame in &frames {
                if ws.send(send(frame)).await.is_err() {
                    return;
                }
            }
            std::future::pending::<()>().await;
        });
        let (socket, _response) = connect_async_with_config(format!("ws://{addr}"), None, false)
            .await
            .unwrap();
        socket
    }

    fn to_json(status: &FleetStatus) -> Value {
        serde_json::to_value(status).unwrap()
    }

    #[test]
    fn the_input_schema_is_derived_from_the_empty_closed_args_type() {
        let schema = Value::Object(tool_for::<FleetStatusTool>().input_schema.as_ref().clone());
        assert_eq!(schema["type"], "object", "{schema}");
        // schemars omits `properties` for a struct with no fields.
        assert!(
            schema.get("properties").is_none_or(|p| p == &json!({})),
            "{schema}"
        );
        assert_eq!(schema["additionalProperties"], json!(false), "{schema}");
        assert!(
            schema.get("required").is_none_or(|r| r == &json!([])),
            "{schema}"
        );
    }

    #[tokio::test]
    async fn the_query_path_keeps_every_host_state_distinct_and_refuses_a_misattributed_answer() {
        let projects = vec![ProjectEntry {
            id: ProjectId::try_from("sealmesh").unwrap(),
            repos: vec![RepoSlug::try_from("example-org/sealmesh").unwrap()],
        }];
        let socket = fake_router(
            projects,
            vec![
                report(
                    "sandbank",
                    HostState::Answering {
                        snapshot: snapshot("sandbank"),
                    },
                ),
                report("shelter", HostState::NotSharing {}),
                report("closed-laptop", HostState::Unreachable {}),
                report("asked-too-often", HostState::Throttled {}),
                // Attributed to `magenta`, claiming to be `sandbank` (rule 21).
                report(
                    "magenta",
                    HostState::Answering {
                        snapshot: snapshot("sandbank"),
                    },
                ),
                ServerFrame::Complete { id: QueryId(1) },
            ],
        )
        .await;

        let status = to_json(&query(socket).await.unwrap());
        assert_eq!(
            status,
            json!({
                "partial": false,
                "undecodable": 0,
                "hosts": [
                    {
                        "state": "answering",
                        "host": "sandbank",
                        "snapshot": {
                            "schema": 2,
                            "host": "sandbank",
                            "collected_at": "2026-08-10T19:56:51Z",
                            "agents": [{
                                "id": "a1b2c3d4",
                                "repo": "example-org/sealmesh",
                                "run_state": "working",
                                "branch": "feature/x",
                                "tasks": { "completed": 1, "total": 3 },
                                "pr": 7,
                                "started_at": "2026-08-10T19:00:00Z",
                                "updated_at": "2026-08-10T19:56:00Z"
                            }]
                        }
                    },
                    { "state": "not_sharing", "host": "shelter" },
                    { "state": "unreachable", "host": "closed-laptop" },
                    { "state": "throttled", "host": "asked-too-often" },
                    { "state": "unreadable", "host": "magenta" }
                ],
                "projects": [{ "id": "sealmesh", "repos": ["example-org/sealmesh"] }]
            })
        );
    }

    #[tokio::test]
    async fn a_partial_frame_is_a_field_so_a_narrowed_answer_is_not_an_empty_fleet() {
        // A machine whose every agent was withheld looks idle, exactly like a
        // machine running nothing. The only difference is this bit.
        let idle = Box::new(
            Snapshot::from_json(
                br#"{"schema":2,"host":"sandbank","collected_at":"2026-08-10T19:56:51Z","agents":[]}"#,
            )
            .unwrap(),
        );
        let frames = |partial: bool| {
            let mut frames = vec![report(
                "sandbank",
                HostState::Answering {
                    snapshot: idle.clone(),
                },
            )];
            if partial {
                frames.push(ServerFrame::Partial { id: QueryId(1) });
            }
            frames.push(ServerFrame::Complete { id: QueryId(1) });
            frames
        };

        let narrowed = to_json(
            &query(fake_router(vec![], frames(true)).await)
                .await
                .unwrap(),
        );
        let quiet = to_json(
            &query(fake_router(vec![], frames(false)).await)
                .await
                .unwrap(),
        );
        assert_eq!(narrowed["partial"], json!(true), "{narrowed}");
        assert_eq!(quiet["partial"], json!(false), "{quiet}");
        assert_eq!(narrowed["hosts"], quiet["hosts"]);
    }

    #[tokio::test]
    async fn an_undecodable_answer_is_counted_not_dropped() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = ws.next().await;
            let text = |s: String| Message::Text(s.into());
            ws.send(text(
                serde_json::to_string(&ServerFrame::Projects { projects: vec![] }).unwrap(),
            ))
            .await
            .unwrap();
            let _ = ws.next().await;
            ws.send(text(
                r#"{"type":"report","id":1,"host":"x","state":{"kind":"newer"}}"#.to_owned(),
            ))
            .await
            .unwrap();
            ws.send(text(
                serde_json::to_string(&ServerFrame::Complete { id: QueryId(1) }).unwrap(),
            ))
            .await
            .unwrap();
            std::future::pending::<()>().await;
        });
        let (socket, _) = connect_async_with_config(format!("ws://{addr}"), None, false)
            .await
            .unwrap();
        let status = to_json(&query(socket).await.unwrap());
        assert_eq!(status["undecodable"], json!(1), "{status}");
        assert_eq!(status["hosts"], json!([]), "{status}");
    }

    #[tokio::test]
    async fn more_hosts_than_the_cap_is_refused_rather_than_held() {
        let mut frames: Vec<ServerFrame> = (0..=MAX_HOSTS)
            .map(|n| report(&format!("host-{n}"), HostState::Unreachable {}))
            .collect();
        frames.push(ServerFrame::Complete { id: QueryId(1) });
        let refused = query(fake_router(vec![], frames).await).await.unwrap_err();
        assert_eq!(refused, FleetStatusFailure::TooManyHosts);

        let mut at_cap: Vec<ServerFrame> = (0..MAX_HOSTS)
            .map(|n| report(&format!("host-{n}"), HostState::Unreachable {}))
            .collect();
        at_cap.push(ServerFrame::Complete { id: QueryId(1) });
        let status = query(fake_router(vec![], at_cap).await).await.unwrap();
        assert_eq!(status.hosts.len(), MAX_HOSTS);
    }

    #[tokio::test]
    async fn a_router_that_refuses_partway_is_a_query_failure() {
        let socket = fake_router(vec![], vec![ServerFrame::Denied {}]).await;
        assert_eq!(
            query(socket).await.unwrap_err(),
            FleetStatusFailure::QueryFailed
        );
    }

    #[tokio::test]
    async fn a_router_that_never_completes_times_out() {
        let started = std::time::Instant::now();
        let never = std::future::pending::<Result<(), FleetStatusFailure>>();
        assert_eq!(
            bounded(Duration::from_millis(50), never).await,
            Err(FleetStatusFailure::TimedOut)
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(
            QUERY_TIMEOUT <= Duration::from_secs(60),
            "{QUERY_TIMEOUT:?}"
        );
    }

    #[tokio::test]
    async fn a_missing_config_is_a_structured_failure_and_nothing_else_is_attempted() {
        let router = RouterAccess::new(Some(std::path::PathBuf::from(
            "/nonexistent/sealmesh-mcp-test/config.toml",
        )));
        let failure = FleetStatusTool::run(router, FleetStatusArgs {})
            .await
            .unwrap_err();
        assert_eq!(failure, FleetStatusFailure::ConfigUnavailable);
        assert_eq!(
            serde_json::to_value(failure).unwrap(),
            json!({ "error": "config_unavailable" })
        );
    }

    #[test]
    fn the_description_names_every_state_the_output_can_carry() {
        // The description is what an agent reads to interpret the result, so
        // a state it does not name is a state the agent cannot act on. The
        // match makes a new variant fail to compile here until it is listed.
        let named = |entry: &HostEntry| match entry {
            HostEntry::Answering { .. }
            | HostEntry::NotSharing { .. }
            | HostEntry::Unreachable { .. }
            | HostEntry::Throttled { .. }
            | HostEntry::Unreadable { .. } => serde_json::to_value(entry).unwrap()["state"]
                .as_str()
                .unwrap()
                .to_owned(),
        };
        for entry in [
            HostEntry::Answering {
                host: host("sandbank"),
                snapshot: snapshot("sandbank"),
            },
            HostEntry::NotSharing {
                host: host("sandbank"),
            },
            HostEntry::Unreachable {
                host: host("sandbank"),
            },
            HostEntry::Throttled {
                host: host("sandbank"),
            },
            HostEntry::Unreadable {
                host: host("sandbank"),
            },
        ] {
            let state = named(&entry);
            assert!(
                FleetStatusTool::DESCRIPTION.contains(&format!("{state},"))
                    || FleetStatusTool::DESCRIPTION.contains(&format!("{state}."))
                    || FleetStatusTool::DESCRIPTION.contains(&format!("{state} ")),
                "the description does not name `{state}`"
            );
        }
        assert!(FleetStatusTool::DESCRIPTION.contains("`partial`"));
    }

    #[test]
    fn every_failure_serialises_as_a_category_and_nothing_else() {
        for (failure, code) in [
            (FleetStatusFailure::ConfigUnavailable, "config_unavailable"),
            (FleetStatusFailure::RouterUnavailable, "router_unavailable"),
            (FleetStatusFailure::QueryFailed, "query_failed"),
            (FleetStatusFailure::TooManyHosts, "too_many_hosts"),
            (FleetStatusFailure::TimedOut, "timed_out"),
            (FleetStatusFailure::RateLimited, "rate_limited"),
            (FleetStatusFailure::OutputTooLarge, "output_too_large"),
        ] {
            assert_eq!(
                serde_json::to_value(failure).unwrap(),
                json!({ "error": code })
            );
        }
    }

    fn no_router() -> impl Fn() -> std::future::Ready<anyhow::Result<Connected>> {
        || std::future::ready(Err(anyhow::anyhow!("no router in this test")))
    }

    #[tokio::test]
    async fn calls_inside_the_cooldown_never_open_a_connection() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let gate = Cooldown::new(COOLDOWN);
        let opened = AtomicUsize::new(0);
        let connect = || {
            opened.fetch_add(1, Ordering::SeqCst);
            no_router()()
        };
        let t0 = Instant::now();

        assert_eq!(
            reach(&gate, t0, connect).await.unwrap_err(),
            FleetStatusFailure::RouterUnavailable
        );
        assert_eq!(opened.load(Ordering::SeqCst), 1);

        // A loop re-calling as fast as it can, and a patient one re-calling at
        // exactly the daemon's refill interval, are both refused.
        let mut inside = vec![Duration::ZERO; 1000];
        inside.extend([
            QUERY_REFILL,
            QUERY_REFILL * 2,
            COOLDOWN - Duration::from_nanos(1),
        ]);
        for offset in inside {
            assert_eq!(
                reach(&gate, t0 + offset, connect).await.unwrap_err(),
                FleetStatusFailure::RateLimited,
                "a call {offset:?} after the last was let through"
            );
        }
        assert_eq!(
            opened.load(Ordering::SeqCst),
            1,
            "a refused call opened a connection"
        );

        // Refusals did not push the window out: the call at the boundary goes.
        assert_eq!(
            reach(&gate, t0 + COOLDOWN, connect).await.unwrap_err(),
            FleetStatusFailure::RouterUnavailable
        );
        assert_eq!(opened.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn calls_made_in_parallel_share_one_claim() {
        use std::sync::{
            Barrier,
            atomic::{AtomicUsize, Ordering},
        };

        // Every thread waits at the barrier, so all of them reach `claim`
        // together rather than one after another, and every round starts on a
        // fresh gate. A claim that checked and recorded under two separate
        // lock acquisitions lets more than one call through in some round.
        const THREADS: usize = 16;
        const ROUNDS: usize = 64;
        let now = Instant::now();
        for round in 0..ROUNDS {
            let gate = Cooldown::new(COOLDOWN);
            let start = Barrier::new(THREADS);
            let granted = AtomicUsize::new(0);
            std::thread::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn(|| {
                        start.wait();
                        if gate.claim(now) {
                            granted.fetch_add(1, Ordering::SeqCst);
                        }
                    });
                }
            });
            assert_eq!(
                granted.into_inner(),
                1,
                "round {round} let more than one parallel call through"
            );
        }
    }

    #[test]
    fn a_clock_reading_earlier_than_the_last_call_is_refused() {
        let gate = Cooldown::new(COOLDOWN);
        let t0 = Instant::now();
        assert!(gate.claim(t0 + Duration::from_secs(60)));
        assert!(!gate.claim(t0));
    }

    #[tokio::test]
    async fn the_tool_entry_point_answers_from_the_one_process_wide_cooldown() {
        // The only test that touches the process-wide gate. It is spent here
        // the way a call that went through would spend it. Then every call
        // through the real entry point, each with its own fresh
        // `RouterAccess`, must be refused as `rate_limited`. A cooldown held
        // per call or per handle would let them through, and they would come
        // back `router_unavailable` from the test build's `open_reader`, which
        // never runs `gh` or dials anything. The config is this test's own
        // file, so no real config is read either.
        let path =
            std::env::temp_dir().join(format!("sealmesh-mcp-cooldown-{}.toml", std::process::id()));
        std::fs::write(
            &path,
            "ticket_prefixes = [\"ENG\"]\n[server]\nurl = \"https://router.invalid\"\n\
             [allow]\nrepos = [\"example-org/sealmesh\"]\npaths = [\"/tmp\"]\n",
        )
        .unwrap();

        let _ = GATE.claim(Instant::now());
        let mut outcomes = Vec::new();
        for _ in 0..3 {
            let access = RouterAccess::new(Some(path.clone()));
            outcomes.push(FleetStatusTool::run(access, FleetStatusArgs {}).await.err());
        }
        let _ = std::fs::remove_file(&path);

        assert_eq!(outcomes, vec![Some(FleetStatusFailure::RateLimited); 3]);
    }

    #[test]
    fn the_cooldown_sits_above_its_floor_and_the_description_states_it() {
        assert!(
            COOLDOWN >= COOLDOWN_FLOOR,
            "{COOLDOWN:?} < {COOLDOWN_FLOOR:?}"
        );
        assert!(COOLDOWN_FLOOR >= QUERY_REFILL * 2, "{COOLDOWN_FLOOR:?}");
        let stated = format!("less than {} seconds apart", COOLDOWN.as_secs());
        assert!(
            FleetStatusTool::DESCRIPTION.contains(&stated),
            "the description does not state the {COOLDOWN:?} cooldown"
        );
        assert!(FleetStatusTool::DESCRIPTION.contains("rate_limited"));
    }

    #[test]
    fn a_looping_agent_cannot_drain_a_daemon_even_with_a_person_asking_too() {
        use sealmesh::daemon::QueryBudget;

        let t0 = Instant::now();
        let step = Duration::from_millis(100);
        let person_every = 100; // steps: one `who` every ten seconds

        // One looping agent and a person, then four looping agents alone.
        for (agents, person) in [(1, true), (4, false)] {
            let mut budget = QueryBudget::standard(t0);
            let gates: Vec<Cooldown> = (0..agents).map(|_| Cooldown::new(COOLDOWN)).collect();
            for tick in 0u32..36_000 {
                let now = t0 + step * tick;
                for gate in &gates {
                    if gate.claim(now) {
                        assert!(
                            budget.allow(now),
                            "{agents} agent(s) drained the budget at {:?}",
                            now - t0
                        );
                    }
                }
                if person && tick % person_every == 0 {
                    assert!(budget.allow(now), "a person was refused at {:?}", now - t0);
                }
            }
        }
    }

    #[test]
    fn a_full_fleet_at_the_host_cap_fits_the_output_budget_when_hosts_are_ordinary() {
        // Not a guarantee — a snapshot may be far larger than this one — but the
        // budget must not refuse an ordinary fleet at the cap, or the cap is a
        // fiction. An oversized answer is refused by the budget instead.
        let status = FleetStatus {
            partial: false,
            undecodable: 0,
            hosts: (0..MAX_HOSTS)
                .map(|n| HostEntry::Unreachable {
                    host: host(&format!("host-{n}")),
                })
                .collect(),
            projects: vec![],
        };
        let result =
            rmcp::model::CallToolResult::structured(serde_json::to_value(&status).unwrap());
        assert!(fits(&result));
    }
}
