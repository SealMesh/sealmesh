//! `room_join`: join the rooms of the checkout this agent is working in.
//!
//! # It takes no room (Q5)
//!
//! The rooms are resolved from the caller's own checkout — the server's
//! working directory, read with `gitref::read` — by the running daemon, from
//! the project map the router sent it. The ticket room (when the branch names
//! a ticket) and the repo room are joined automatically. The project room is
//! joined only when the caller passes `project: true` *and* `[chat]
//! project_rooms` is on (Q3).
//!
//! Its one input, `project`, is a bool. It can widen the join to the caller's
//! own project room and cannot name any other room, agent, host or session.
//!
//! # A refused join reveals nothing (rule 25)
//!
//! Each room is joined through the daemon's ordinary join path, so the router
//! decides every one. A refused room is reported by its scope word and
//! `refused`, without the room itself: which room the router would not let
//! this machine into, and why, is not the caller's to learn.

use std::time::{Duration, Instant};

use rmcp::{model::ToolAnnotations, schemars::JsonSchema};
use sealmesh::room::local::{JoinOutcome, Request, Response};
use sealmesh_chat_wire::RoomId;
use serde::{Deserialize, Serialize};

use super::{
    McpTool,
    cooldown::Cooldown,
    rooms::{Context, RoomFailure, RoomScope, bounded},
};
use crate::router::RouterAccess;

/// The least time between two `room_join` calls in one process. Each call
/// can cost the router one GitHub access check per room, charged to this
/// machine, and joining is something an agent does once per task.
pub const COOLDOWN: Duration = Duration::from_secs(10);

static GATE: Cooldown = Cooldown::new(COOLDOWN);

pub struct RoomJoinTool;

#[derive(Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub struct RoomJoinArgs {
    /// Also join this checkout's project room, which spans every repo in the
    /// project. Only when `[chat] project_rooms` is on. Defaults to false.
    #[serde(default)]
    pub project: bool,
}

/// Which rooms were joined.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct RoomsJoined {
    /// Ticket room first, then repo, then project.
    pub rooms: Vec<JoinedRoom>,
    pub project: ProjectRoom,
}

/// One of the checkout's rooms and how its join went: the daemon's answer in
/// `outcome`, or, when no answer came, why not in `failed`. Exactly one of
/// the two is present.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct JoinedRoom {
    pub scope: RoomScope,
    /// `joined`, `refused`, `room_limit`, `busy`, `timed_out`,
    /// `not_connected` or `disabled`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<JoinOutcome>,
    /// The join got no answer, and this is why: the daemon went away, or the
    /// call ran out of time before or while asking. The rooms before it keep
    /// their own outcomes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed: Option<RoomFailure>,
    /// The room, only once it is joined. A refused room is not named
    /// (rule 25).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub room: Option<RoomId>,
}

/// What happened to the project room.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectRoom {
    /// `project` was not passed.
    NotRequested,
    /// `project` was passed, and `[chat] project_rooms` is off here or on
    /// the daemon, so it was not joined.
    NotEnabled,
    /// It is in `rooms`, with its own outcome.
    Included,
}

impl McpTool for RoomJoinTool {
    const NAME: &'static str = "room_join";
    const DESCRIPTION: &'static str = "Join the sealmesh chat rooms of the checkout you are working \
         in: the room of the ticket your branch names, and the room of your repo. Takes no room: \
         they are worked out from your working directory. Pass project: true to also join your \
         project's room, which only works when [chat] project_rooms is on. Each room's outcome is \
         joined, refused, room_limit, busy, timed_out, not_connected or disabled; a refused room \
         is not named. A room whose join got no answer has `failed` with an error instead, and \
         the rooms before it keep their outcomes. `project` says not_requested, not_enabled or \
         included. Fails with error rate_limited (calls less than 10 seconds apart), config_unavailable, not_a_checkout, \
         no_room, daemon_not_running, daemon_untrusted, daemon_refused, daemon_error, disabled, \
         not_connected, busy, timed_out or output_too_large.";

    type Args = RoomJoinArgs;
    type Output = RoomsJoined;
    type Failure = RoomFailure;

    fn annotations() -> ToolAnnotations {
        ToolAnnotations::with_title("Join this checkout's rooms")
            .read_only(false)
            .destructive(false)
            .idempotent(true)
            .open_world(true)
    }

    fn output_too_large() -> RoomFailure {
        RoomFailure::OutputTooLarge
    }

    async fn run(router: RouterAccess, args: RoomJoinArgs) -> Result<RoomsJoined, RoomFailure> {
        let context = Context::from_process(&router)?;
        join(&context, &GATE, Instant::now(), args).await
    }

    #[cfg(test)]
    fn complete_sample() -> serde_json::Value {
        serde_json::json!({ "project": true })
    }
}

/// Everything after the config is read. The cooldown is claimed only once the
/// checkout is known, so a call from a directory that is no checkout does not
/// spend it, and before anything is sent to the daemon.
pub(crate) async fn join(
    context: &Context,
    gate: &Cooldown,
    now: Instant,
    RoomJoinArgs { project }: RoomJoinArgs,
) -> Result<RoomsJoined, RoomFailure> {
    let checkout = context.checkout()?;
    if !gate.claim(now) {
        return Err(RoomFailure::RateLimited);
    }
    let deadline = tokio::time::Instant::now() + context.call_deadline;
    let resolved = bounded(context.call_deadline, context.resolve(&checkout)).await?;
    let project_named = resolved
        .iter()
        .any(|room| RoomScope::of(room) == RoomScope::Project);
    let project_state = match (project, project_named) {
        (false, _) => ProjectRoom::NotRequested,
        (true, false) => ProjectRoom::NotEnabled,
        (true, true) => ProjectRoom::Included,
    };
    let wanted: Vec<RoomId> = resolved
        .into_iter()
        .filter(|room| project || RoomScope::of(room) != RoomScope::Project)
        .collect();
    if wanted.is_empty() {
        return Err(RoomFailure::NoRoom);
    }
    // Each room answers for itself: a join that gets no answer is reported on
    // that room, and does not discard the joins before it.
    let mut rooms = Vec::with_capacity(wanted.len());
    for room in wanted {
        let scope = RoomScope::of(&room);
        let asked =
            tokio::time::timeout_at(deadline, context.ask(&Request::Join { room, holder: None }))
                .await;
        rooms.push(match asked {
            Ok(Ok(response)) => {
                let outcome = Some(JoinOutcome::of(&response));
                let room = match response {
                    Response::Joined { room } => Some(room),
                    _ => None,
                };
                JoinedRoom {
                    scope,
                    outcome,
                    failed: None,
                    room,
                }
            }
            Ok(Err(failure)) => JoinedRoom {
                scope,
                outcome: None,
                failed: Some(failure),
                room: None,
            },
            Err(_) => JoinedRoom {
                scope,
                outcome: None,
                failed: Some(RoomFailure::TimedOut),
                room: None,
            },
        });
    }
    Ok(RoomsJoined {
        rooms,
        project: project_state,
    })
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use sealmesh_wire::{RepoSlug, TicketId};
    use serde_json::json;

    use super::*;
    use crate::tools::{
        rooms::fake::{FakeDaemon, project_room, repo_room, resolved, ticket_room},
        tests::no_config,
        tool_for,
    };

    fn open() -> Cooldown {
        Cooldown::new(COOLDOWN)
    }

    /// Joins every room it is asked to, and resolves the checkout to `rooms`.
    fn fake(tag: &str, rooms: Vec<RoomId>) -> FakeDaemon {
        FakeDaemon::start(tag, move |request| match request {
            Request::Resolve { .. } => Some(resolved(&rooms)),
            Request::Join { room, .. } => Some(Response::Joined { room: room.clone() }),
            _ => Some(Response::Invalid {}),
        })
    }

    #[test]
    fn the_input_schema_takes_no_room_and_nothing_but_project() {
        let schema =
            serde_json::Value::Object(tool_for::<RoomJoinTool>().input_schema.as_ref().clone());
        let fields: Vec<&String> = schema["properties"].as_object().unwrap().keys().collect();
        assert_eq!(fields, ["project"], "{schema}");
        assert_eq!(schema["properties"]["project"]["type"], json!("boolean"));
        assert!(
            serde_json::from_value::<RoomJoinArgs>(json!({ "room": "ticket" })).is_err(),
            "a room argument must be refused"
        );
        assert_eq!(
            serde_json::from_value::<RoomJoinArgs>(json!({})).unwrap(),
            RoomJoinArgs { project: false }
        );
    }

    #[tokio::test]
    async fn a_join_resolves_the_checkout_it_runs_in_and_joins_its_ticket_and_repo_rooms() {
        let mut daemon = fake("j1", vec![ticket_room(), repo_room()]);
        let context = daemon.context(false);
        let joined = join(
            &context,
            &open(),
            Instant::now(),
            RoomJoinArgs { project: false },
        )
        .await
        .unwrap();
        // The repo and the ticket come from the checkout's origin and branch,
        // and nothing asked the daemon to join as part of resolving.
        assert_eq!(
            daemon.requests(),
            vec![
                Request::Resolve {
                    repo: RepoSlug::try_from("example-org/example-repo").unwrap(),
                    ticket: Some(TicketId::new("ENG", 7).unwrap()),
                    join: false,
                },
                Request::Join {
                    room: ticket_room(),
                    holder: None
                },
                Request::Join {
                    room: repo_room(),
                    holder: None
                },
            ]
        );
        assert_eq!(
            serde_json::to_value(&joined).unwrap(),
            json!({
                "rooms": [
                    { "scope": "ticket", "outcome": "joined", "room": ticket_room() },
                    { "scope": "repo", "outcome": "joined", "room": repo_room() },
                ],
                "project": "not_requested",
            })
        );
    }

    #[tokio::test]
    async fn the_project_room_is_joined_only_with_the_opt_in_and_the_config_both() {
        let all = vec![ticket_room(), repo_room(), project_room()];
        for (project, project_rooms, joins_project, state) in [
            (false, false, false, ProjectRoom::NotRequested),
            (false, true, false, ProjectRoom::NotRequested),
            (true, false, false, ProjectRoom::NotEnabled),
            (true, true, true, ProjectRoom::Included),
        ] {
            let mut daemon = fake("j2", all.clone());
            let context = daemon.context(project_rooms);
            let joined = join(&context, &open(), Instant::now(), RoomJoinArgs { project })
                .await
                .unwrap();
            let asked_project = daemon.requests().contains(&Request::Join {
                room: project_room(),
                holder: None,
            });
            assert_eq!(asked_project, joins_project, "{project} {project_rooms}");
            assert_eq!(joined.project, state, "{project} {project_rooms}");
            assert_eq!(
                joined
                    .rooms
                    .iter()
                    .any(|room| room.scope == RoomScope::Project),
                joins_project
            );
        }
    }

    #[tokio::test]
    async fn the_opt_in_with_a_daemon_that_names_no_project_room_is_not_enabled() {
        // `[chat] project_rooms` is on here but off on the daemon.
        let mut daemon = fake("j3", vec![repo_room()]);
        let context = daemon.context(true);
        let joined = join(
            &context,
            &open(),
            Instant::now(),
            RoomJoinArgs { project: true },
        )
        .await
        .unwrap();
        assert_eq!(joined.project, ProjectRoom::NotEnabled);
        assert_eq!(joined.rooms.len(), 1);
    }

    #[tokio::test]
    async fn a_refused_join_does_not_name_the_room() {
        let mut daemon = FakeDaemon::start("j4", |request| match request {
            Request::Resolve { .. } => Some(resolved(&[ticket_room(), repo_room()])),
            Request::Join { room, .. } if *room == ticket_room() => {
                Some(Response::JoinRefused { room: room.clone() })
            }
            Request::Join { room, .. } => Some(Response::Joined { room: room.clone() }),
            _ => None,
        });
        let context = daemon.context(false);
        let joined = join(
            &context,
            &open(),
            Instant::now(),
            RoomJoinArgs { project: false },
        )
        .await
        .unwrap();
        let encoded = serde_json::to_value(&joined).unwrap();
        assert_eq!(
            encoded["rooms"][0],
            json!({ "scope": "ticket", "outcome": "refused" })
        );
        assert!(!encoded.to_string().contains("ENG-7"), "{encoded}");
        assert_eq!(encoded["rooms"][1]["outcome"], json!("joined"));
    }

    #[tokio::test]
    async fn each_join_outcome_is_reported_as_its_own_word() {
        for (response, word) in [
            (Response::RoomLimit {}, "room_limit"),
            (Response::Busy {}, "busy"),
            (Response::TimedOut {}, "timed_out"),
            (Response::NotConnected {}, "not_connected"),
            (Response::Disabled {}, "disabled"),
        ] {
            let mut daemon = FakeDaemon::start("j5", move |request| match request {
                Request::Resolve { .. } => Some(resolved(&[repo_room()])),
                _ => Some(response.clone()),
            });
            let context = daemon.context(false);
            let joined = join(
                &context,
                &open(),
                Instant::now(),
                RoomJoinArgs { project: false },
            )
            .await
            .unwrap();
            let encoded = serde_json::to_value(&joined).unwrap();
            assert_eq!(
                encoded["rooms"][0],
                json!({ "scope": "repo", "outcome": word })
            );
        }
    }

    #[tokio::test]
    async fn a_checkout_in_no_room_is_no_room_and_a_resolve_refusal_keeps_its_category() {
        let mut daemon = fake("j6", vec![]);
        let context = daemon.context(false);
        assert_eq!(
            join(
                &context,
                &open(),
                Instant::now(),
                RoomJoinArgs { project: true }
            )
            .await,
            Err(RoomFailure::NoRoom)
        );
        // A project room alone, without the opt-in, is no room either.
        let mut daemon = daemon_with_project_only();
        let context = daemon.context(true);
        assert_eq!(
            join(
                &context,
                &open(),
                Instant::now(),
                RoomJoinArgs { project: false }
            )
            .await,
            Err(RoomFailure::NoRoom)
        );
        for (response, failure) in [
            (Response::Disabled {}, RoomFailure::Disabled),
            (Response::NotConnected {}, RoomFailure::NotConnected),
            (Response::Busy {}, RoomFailure::Busy),
            (Response::TimedOut {}, RoomFailure::TimedOut),
            (Response::Invalid {}, RoomFailure::DaemonError),
        ] {
            let mut daemon = FakeDaemon::start("j7", move |_| Some(response.clone()));
            let context = daemon.context(false);
            assert_eq!(
                join(
                    &context,
                    &open(),
                    Instant::now(),
                    RoomJoinArgs { project: false }
                )
                .await,
                Err(failure)
            );
        }
    }

    fn daemon_with_project_only() -> FakeDaemon {
        fake("j8", vec![project_room()])
    }

    #[tokio::test]
    async fn calls_inside_the_cooldown_send_nothing_to_the_daemon() {
        let mut daemon = fake("j9", vec![repo_room()]);
        let context = daemon.context(false);
        let gate = open();
        let start = Instant::now();
        join(&context, &gate, start, RoomJoinArgs { project: false })
            .await
            .unwrap();
        let sent = daemon.requests().len();
        for offset in [
            Duration::ZERO,
            COOLDOWN / 2,
            COOLDOWN - Duration::from_millis(1),
        ] {
            assert_eq!(
                join(
                    &context,
                    &gate,
                    start + offset,
                    RoomJoinArgs { project: false }
                )
                .await,
                Err(RoomFailure::RateLimited)
            );
        }
        assert_eq!(
            daemon.requests().len(),
            sent,
            "a refused call reached the daemon"
        );
        join(
            &context,
            &gate,
            start + COOLDOWN,
            RoomJoinArgs { project: false },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_daemon_that_stops_answering_cannot_hold_the_call_past_its_deadline() {
        let mut daemon = FakeDaemon::start("ja", |request| match request {
            Request::Resolve { .. } => Some(resolved(&[ticket_room(), repo_room()])),
            _ => None,
        });
        let mut context = daemon.context(false);
        context.call_deadline = Duration::from_millis(300);
        let started = std::time::Instant::now();
        let joined = join(
            &context,
            &open(),
            Instant::now(),
            RoomJoinArgs { project: false },
        )
        .await
        .unwrap();
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(
            joined
                .rooms
                .iter()
                .all(|room| room.failed == Some(RoomFailure::TimedOut) && room.outcome.is_none()),
            "{joined:?}"
        );
        let mut daemon = FakeDaemon::start("jb", |_| None);
        let mut context = daemon.context(false);
        context.call_deadline = Duration::from_millis(300);
        assert_eq!(
            join(
                &context,
                &open(),
                Instant::now(),
                RoomJoinArgs { project: false }
            )
            .await,
            Err(RoomFailure::TimedOut),
            "a resolve that never answers joins nothing"
        );
    }

    #[tokio::test]
    async fn a_join_that_fails_part_way_keeps_the_rooms_joined_before_it() {
        // The ticket room joins; the daemon then stops answering the repo
        // room's join before the request deadline.
        let mut daemon = FakeDaemon::start("jc", |request| match request {
            Request::Resolve { .. } => Some(resolved(&[ticket_room(), repo_room()])),
            Request::Join { room, .. } if *room == ticket_room() => {
                Some(Response::Joined { room: room.clone() })
            }
            _ => None,
        });
        let mut context = daemon.context(false);
        context.request_deadline = Duration::from_millis(200);
        let joined = join(
            &context,
            &open(),
            Instant::now(),
            RoomJoinArgs { project: false },
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::to_value(&joined).unwrap()["rooms"],
            json!([
                { "scope": "ticket", "outcome": "joined", "room": ticket_room() },
                { "scope": "repo", "failed": { "error": "timed_out" } },
            ])
        );
    }

    #[tokio::test]
    async fn the_tool_entry_point_without_a_config_is_config_unavailable_and_sends_nothing() {
        assert_eq!(
            RoomJoinTool::run(no_config(), RoomJoinArgs { project: true }).await,
            Err(RoomFailure::ConfigUnavailable)
        );
    }
}
