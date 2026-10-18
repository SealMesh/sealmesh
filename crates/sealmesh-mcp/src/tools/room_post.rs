//! `room_post`: send one typed message to one of the caller's own rooms.
//!
//! # Inputs, and why none can name another agent (Q5)
//!
//! - `room` is a scope word — `ticket`, `repo` or `project` — resolved
//!   against the caller's own checkout. It picks one of the caller's own
//!   rooms and cannot name another; it is never a free-form room id.
//! - `kind` is one of `question`, `claim`, `handoff`, `ack`: the chat wire's
//!   closed `MessageKind`.
//! - `body` is the message. It goes through the chat wire's `Body::new`, so
//!   an empty or over-cap body is refused by the constructor, never
//!   truncated (rules 9, 45), and it names no one by construction of the
//!   protocol: a room delivers to its members, not to an addressee.
//!
//! The drain consumer key is **not** an input. It is derived from the Claude
//! Code session that spawned this server (see [`crate::session`]), so an
//! agent's own post is not drained back to it, and no agent can name another
//! agent's key.
//!
//! # Three outcomes, kept apart
//!
//! `posted` carries the sequence number the router recorded it under.
//! `post_refused` is a failure carrying the router's category — `unknown`
//! included, for a category a newer router sends. `post_unconfirmed` means
//! the post was sent and neither a delivery nor a refusal came back in time:
//! it may or may not have been recorded, and a retry may duplicate it. It is
//! reported as its own outcome, never as `posted` and never as a refusal,
//! and it tells the agent to look with `room_read` before posting again.
//!
//! The same holds for a post this server gave up on: once the `Post` request
//! may have reached the daemon, any failure to hear back — a deadline, a
//! reset, an answer that does not decode — is `post_unconfirmed`, never
//! `timed_out`. `timed_out` stays what the daemon means by it: the post was
//! never sent, and retrying is safe. So the post runs under its own request
//! deadline, after the resolve, and the call deadline that bounds the resolve
//! can never expire on a post that was already written.
//!
//! # The drain key is looked up last, off the runtime
//!
//! Only once the body is valid and the cooldown is claimed, and on a
//! blocking thread under [`crate::session::LOOKUP_DEADLINE`]: a lookup that
//! stalls posts without a key rather than stalling the server.

use std::time::{Duration, Instant};

use rmcp::{model::ToolAnnotations, schemars::JsonSchema};
use sealmesh::room::local::{ConsumerKey, Request, Response};
use sealmesh_chat_wire::{Body, MAX_BODY_CHARS, MessageKind, RoomId};
use sealmesh_proto::RoomSeq;
use serde::{Deserialize, Serialize};

use super::{
    McpTool,
    cooldown::Cooldown,
    rooms::{Context, RoomFailure, RoomScope, bounded, may_have_arrived, refusal, unreachable},
};
use crate::{router::RouterAccess, session};

/// The least time between two `room_post` calls in one process: the router's
/// own sustained post rate, one every two seconds. A loop past it would only
/// be refused by the router, after spending a round trip.
pub const COOLDOWN: Duration = Duration::from_secs(2);

static GATE: Cooldown = Cooldown::new(COOLDOWN);

pub struct RoomPostTool;

/// The four kinds a message can be. This is the input's shape, not a
/// redefinition of the wire type: it converts into the chat wire's
/// `MessageKind` and nothing else, and a test holds the two to the same four
/// words.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// Asking for something: information, a decision, a review.
    Question,
    /// Putting a fact on the record, such as what you are taking on.
    Claim,
    /// Passing ownership of something on.
    Handoff,
    /// Acknowledging, with no content of its own.
    Ack,
}

impl From<Kind> for MessageKind {
    fn from(kind: Kind) -> Self {
        match kind {
            Kind::Question => Self::Question,
            Kind::Claim => Self::Claim,
            Kind::Handoff => Self::Handoff,
            Kind::Ack => Self::Ack,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
#[serde(deny_unknown_fields)]
pub struct RoomPostArgs {
    /// Which of your own rooms: `ticket`, `repo` or `project`.
    pub room: RoomScope,
    pub kind: Kind,
    /// The message, 1 to 4,000 characters. A longer one is refused, not cut.
    #[schemars(length(min = 1, max = MAX_BODY_CHARS))]
    pub body: String,
}

/// A post that got an answer other than a refusal.
#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Posted {
    /// The router recorded the message as `seq` and delivered it.
    Posted { room: RoomId, seq: RoomSeq },
    /// Sent, but no answer came back in time. It may or may not have been
    /// recorded, and a retry may duplicate it.
    PostUnconfirmed {
        room: RoomId,
        before_posting_again: Advice,
    },
}

/// What to do about a post that may or may not have been recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Advice {
    /// Read the room with `room_read` and look for the message first.
    CheckWithRoomRead,
}

fn unconfirmed(room: RoomId) -> Posted {
    Posted::PostUnconfirmed {
        room,
        before_posting_again: Advice::CheckWithRoomRead,
    }
}

impl McpTool for RoomPostTool {
    const NAME: &'static str = "room_post";
    const DESCRIPTION: &'static str = "Post one message to one of the sealmesh chat rooms of the \
         checkout you are working in. `room` is ticket, repo or project — which of your own \
         rooms, never another's. `kind` is question, claim, handoff or ack. `body` is 1 to 4000 \
         characters; a longer one fails with body_refused and is not cut. Join first with \
         room_join. The outcome is posted (with the seq it was recorded as) or post_unconfirmed \
         (sent, but no answer came in time: it may or may not have been recorded, and posting \
         again may duplicate it, so check with room_read before posting again). Fails with error post_refused and a category: rate_limited, \
         budget_exhausted, turn_capped, not_recorded, archived or unknown. Other errors: \
         rate_limited (calls less than 2 seconds apart), config_unavailable, not_a_checkout, \
         no_room, body_refused, daemon_not_running, daemon_untrusted, daemon_refused, \
         daemon_error, disabled, not_connected, busy, timed_out, not_member or \
         output_too_large.";

    type Args = RoomPostArgs;
    type Output = Posted;
    type Failure = RoomFailure;

    fn annotations() -> ToolAnnotations {
        ToolAnnotations::with_title("Post to this checkout's room")
            .read_only(false)
            .destructive(false)
            .idempotent(false)
            .open_world(true)
    }

    fn output_too_large() -> RoomFailure {
        RoomFailure::OutputTooLarge
    }

    async fn run(router: RouterAccess, args: RoomPostArgs) -> Result<Posted, RoomFailure> {
        let context = Context::from_process(&router)?;
        post(&context, &GATE, Instant::now(), consumer_key, args).await
    }

    #[cfg(test)]
    fn complete_sample() -> serde_json::Value {
        serde_json::json!({ "room": "ticket", "kind": "claim", "body": "taking the portal" })
    }
}

#[cfg(not(test))]
fn consumer_key() -> Option<ConsumerKey> {
    crate::session::consumer_key()
}

/// Under test, the entry point never reads this machine's Claude Code
/// sessions. `crate::session` is tested against its own fixtures.
#[cfg(test)]
fn consumer_key() -> Option<ConsumerKey> {
    None
}

/// Everything after the config is read. The body and the checkout are
/// checked before the cooldown is claimed, so a call refused for either does
/// not spend it, and nothing is sent to the daemon for a call it refuses.
/// `lookup` finds the drain key, and runs only once all of that has passed.
pub(crate) async fn post<L>(
    context: &Context,
    gate: &Cooldown,
    now: Instant,
    lookup: L,
    RoomPostArgs { room, kind, body }: RoomPostArgs,
) -> Result<Posted, RoomFailure>
where
    L: FnOnce() -> Option<ConsumerKey> + Send + 'static,
{
    let body = Body::new(body).map_err(|_| RoomFailure::BodyRefused)?;
    let checkout = context.checkout()?;
    if !gate.claim(now) {
        return Err(RoomFailure::RateLimited);
    }
    let room = bounded(context.call_deadline, context.room(&checkout, room)).await?;
    let consumer = session::off_runtime(lookup, context.lookup_deadline).await;
    let request = Request::Post {
        room: room.clone(),
        kind: kind.into(),
        body,
        consumer,
    };
    // Not under the call deadline: from here, giving up means not knowing.
    match context.send(&request).await {
        Ok(Response::Posted { room, seq }) => Ok(Posted::Posted { room, seq }),
        Ok(Response::PostUnconfirmed { room }) => Ok(unconfirmed(room)),
        Ok(other) => Err(refusal(other)),
        Err(error) if may_have_arrived(&error) => Ok(unconfirmed(room)),
        Err(error) => Err(unreachable(&error)),
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
    use sealmesh_proto::PostRefusal;
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

    fn no_key() -> Option<ConsumerKey> {
        None
    }

    fn args(room: RoomScope, body: &str) -> RoomPostArgs {
        RoomPostArgs {
            room,
            kind: Kind::Claim,
            body: body.to_owned(),
        }
    }

    /// Resolves to every room, and answers each post with `answer`.
    fn fake(tag: &str, answer: Response) -> FakeDaemon {
        FakeDaemon::start(tag, move |request| match request {
            Request::Resolve { .. } => {
                Some(resolved(&[ticket_room(), repo_room(), project_room()]))
            }
            Request::Post { .. } => Some(answer.clone()),
            _ => Some(Response::Invalid {}),
        })
    }

    #[test]
    fn the_input_schema_is_a_scope_a_kind_and_a_capped_body() {
        let schema =
            serde_json::Value::Object(tool_for::<RoomPostTool>().input_schema.as_ref().clone());
        let mut fields: Vec<&String> = schema["properties"].as_object().unwrap().keys().collect();
        fields.sort();
        assert_eq!(fields, ["body", "kind", "room"], "{schema}");
        assert_eq!(
            schema["properties"]["body"]["maxLength"],
            json!(MAX_BODY_CHARS)
        );
        assert_eq!(schema["properties"]["body"]["minLength"], json!(1));
        let text = schema.to_string();
        for word in [
            "ticket", "repo", "project", "question", "claim", "handoff", "ack",
        ] {
            assert!(text.contains(&format!("\"{word}\"")), "{word}: {schema}");
        }
        // A free-form room id is not a scope word.
        assert!(
            serde_json::from_value::<RoomPostArgs>(json!({
                "room": { "project": "example-repo", "scope": "project" },
                "kind": "claim",
                "body": "x"
            }))
            .is_err()
        );
        // The consumer key is not an input, and neither is a sender.
        for extra in ["consumer", "from"] {
            let mut sample = RoomPostTool::complete_sample();
            sample[extra] = json!("0b3e5c0a-6f7e-4a21-9d1c-5b7c1f0e2a11");
            assert!(
                serde_json::from_value::<RoomPostArgs>(sample).is_err(),
                "{extra} was accepted"
            );
        }
    }

    #[test]
    fn the_input_kinds_are_the_wire_kinds_word_for_word() {
        for kind in [Kind::Question, Kind::Claim, Kind::Handoff, Kind::Ack] {
            let wire = MessageKind::from(kind);
            let word = serde_json::to_value(wire).unwrap();
            assert_eq!(serde_json::from_value::<Kind>(word).unwrap(), kind);
        }
        // Every wire kind has an input kind: a fifth one fails to compile here.
        let _ = |kind: MessageKind| match kind {
            MessageKind::Question
            | MessageKind::Claim
            | MessageKind::Handoff
            | MessageKind::Ack => {}
        };
    }

    #[tokio::test]
    async fn a_post_goes_to_the_room_of_its_scope_with_the_body_and_the_derived_key() {
        let mut daemon = fake(
            "p1",
            Response::Posted {
                room: repo_room(),
                seq: RoomSeq(41),
            },
        );
        let context = daemon.context(false);
        let key = ConsumerKey::new("0b3e5c0a-6f7e-4a21-9d1c-5b7c1f0e2a11").unwrap();
        let posted = post(
            &context,
            &open(),
            Instant::now(),
            {
                let key = key.clone();
                move || Some(key)
            },
            RoomPostArgs {
                room: RoomScope::Repo,
                kind: Kind::Handoff,
                body: "the portal is yours".to_owned(),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::to_value(&posted).unwrap(),
            json!({ "outcome": "posted", "room": repo_room(), "seq": 41 })
        );
        let requests = daemon.requests();
        assert_eq!(
            requests.last(),
            Some(&Request::Post {
                room: repo_room(),
                kind: MessageKind::Handoff,
                body: Body::new("the portal is yours").unwrap(),
                consumer: Some(key),
            })
        );
        assert!(matches!(requests[0], Request::Resolve { join: false, .. }));
    }

    #[tokio::test]
    async fn posted_refused_and_unconfirmed_are_three_different_results() {
        let mut daemon = fake(
            "p2",
            Response::PostUnconfirmed {
                room: ticket_room(),
            },
        );
        let context = daemon.context(false);
        let unconfirmed = post(
            &context,
            &open(),
            Instant::now(),
            no_key,
            args(RoomScope::Ticket, "x"),
        )
        .await
        .unwrap();
        let unconfirmed = serde_json::to_value(unconfirmed).unwrap();
        assert_eq!(
            unconfirmed,
            json!({
                "outcome": "post_unconfirmed",
                "room": ticket_room(),
                "before_posting_again": "check_with_room_read",
            })
        );
        assert!(unconfirmed.get("seq").is_none());

        let mut daemon = fake(
            "p3",
            Response::Posted {
                room: ticket_room(),
                seq: RoomSeq(1),
            },
        );
        let context = daemon.context(false);
        let posted = post(
            &context,
            &open(),
            Instant::now(),
            no_key,
            args(RoomScope::Ticket, "x"),
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::to_value(posted).unwrap()["outcome"],
            json!("posted")
        );

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
            let mut daemon = fake(
                "p4",
                Response::PostRefused {
                    room: ticket_room(),
                    category,
                },
            );
            let context = daemon.context(false);
            let refused = post(
                &context,
                &open(),
                Instant::now(),
                no_key,
                args(RoomScope::Ticket, "x"),
            )
            .await
            .unwrap_err();
            assert_eq!(refused, RoomFailure::PostRefused { category });
            let word = serde_json::to_value(category).unwrap();
            assert_eq!(
                serde_json::to_value(refused).unwrap(),
                json!({ "error": "post_refused", "category": word })
            );
        }
    }

    #[tokio::test]
    async fn every_other_daemon_answer_to_a_post_is_its_own_failure() {
        for (response, failure) in [
            (Response::Disabled {}, RoomFailure::Disabled),
            (Response::NotConnected {}, RoomFailure::NotConnected),
            (Response::Busy {}, RoomFailure::Busy),
            (Response::TimedOut {}, RoomFailure::TimedOut),
            (
                Response::NotMember { room: repo_room() },
                RoomFailure::NotMember,
            ),
            (Response::Invalid {}, RoomFailure::DaemonError),
        ] {
            let mut daemon = fake("p5", response);
            let context = daemon.context(false);
            assert_eq!(
                post(
                    &context,
                    &open(),
                    Instant::now(),
                    no_key,
                    args(RoomScope::Repo, "x")
                )
                .await,
                Err(failure)
            );
        }
    }

    #[tokio::test]
    async fn an_over_cap_or_empty_body_is_refused_whole_and_nothing_is_sent() {
        let mut daemon = fake(
            "p6",
            Response::Posted {
                room: repo_room(),
                seq: RoomSeq(1),
            },
        );
        let context = daemon.context(false);
        let gate = open();
        for body in ["x".repeat(MAX_BODY_CHARS + 1), String::new()] {
            assert_eq!(
                post(
                    &context,
                    &gate,
                    Instant::now(),
                    no_key,
                    args(RoomScope::Repo, &body)
                )
                .await,
                Err(RoomFailure::BodyRefused)
            );
        }
        assert!(daemon.requests().is_empty(), "{:?}", daemon.requests());
        // Refused bodies do not spend the cooldown, and a body exactly at the
        // cap — counted in characters, not bytes — is sent whole.
        let at_cap = "é".repeat(MAX_BODY_CHARS);
        post(
            &context,
            &gate,
            Instant::now(),
            no_key,
            args(RoomScope::Repo, &at_cap),
        )
        .await
        .unwrap();
        let Some(Request::Post { body, .. }) = daemon.requests().last().cloned() else {
            panic!("no post was sent");
        };
        assert_eq!(body.as_str(), at_cap);
    }

    #[tokio::test]
    async fn a_scope_this_checkout_has_no_room_for_is_no_room() {
        let mut daemon = FakeDaemon::start("p7", |request| match request {
            Request::Resolve { .. } => Some(resolved(&[repo_room(), project_room()])),
            _ => Some(Response::Invalid {}),
        });
        // No ticket room was named, and the project room is not on here.
        let context = daemon.context(false);
        for scope in [RoomScope::Ticket, RoomScope::Project] {
            assert_eq!(
                post(&context, &open(), Instant::now(), no_key, args(scope, "x")).await,
                Err(RoomFailure::NoRoom)
            );
        }
        assert!(
            !daemon
                .requests()
                .iter()
                .any(|request| matches!(request, Request::Post { .. }))
        );
    }

    #[tokio::test]
    async fn posts_inside_the_cooldown_send_nothing_to_the_daemon() {
        let mut daemon = fake(
            "p8",
            Response::Posted {
                room: repo_room(),
                seq: RoomSeq(1),
            },
        );
        let context = daemon.context(false);
        let gate = open();
        let start = Instant::now();
        post(&context, &gate, start, no_key, args(RoomScope::Repo, "one"))
            .await
            .unwrap();
        let sent = daemon.requests().len();
        assert_eq!(
            post(
                &context,
                &gate,
                start + COOLDOWN / 2,
                no_key,
                args(RoomScope::Repo, "two")
            )
            .await,
            Err(RoomFailure::RateLimited)
        );
        assert_eq!(daemon.requests().len(), sent);
        post(
            &context,
            &gate,
            start + COOLDOWN,
            no_key,
            args(RoomScope::Repo, "three"),
        )
        .await
        .unwrap();
    }

    fn resolves_then(tag: &str, post_delay: Duration, answer: Option<Response>) -> FakeDaemon {
        FakeDaemon::start_slow(
            tag,
            move |request| match request {
                Request::Post { .. } => post_delay,
                _ => Duration::ZERO,
            },
            move |request| match request {
                Request::Resolve { .. } => Some(resolved(&[ticket_room(), repo_room()])),
                Request::Post { .. } => answer.clone(),
                _ => Some(Response::Invalid {}),
            },
        )
    }

    #[tokio::test]
    async fn a_post_the_daemon_took_and_never_answered_is_unconfirmed_not_timed_out() {
        let mut daemon = resolves_then("pa", Duration::ZERO, None);
        let mut context = daemon.context(false);
        context.request_deadline = Duration::from_millis(300);
        let result = post(
            &context,
            &open(),
            Instant::now(),
            no_key,
            args(RoomScope::Repo, "x"),
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::to_value(&result).unwrap()["outcome"],
            json!("post_unconfirmed")
        );
        assert!(matches!(
            daemon.requests().last(),
            Some(Request::Post { .. })
        ));
    }

    #[tokio::test]
    async fn the_call_deadline_never_expires_on_a_post_already_written() {
        // The resolve is fast; the post is answered after the call deadline
        // has passed but inside its own request deadline.
        let mut daemon = resolves_then(
            "pb",
            Duration::from_millis(600),
            Some(Response::Posted {
                room: repo_room(),
                seq: RoomSeq(9),
            }),
        );
        let mut context = daemon.context(false);
        context.call_deadline = Duration::from_millis(200);
        context.request_deadline = Duration::from_secs(3);
        let result = post(
            &context,
            &open(),
            Instant::now(),
            no_key,
            args(RoomScope::Repo, "x"),
        )
        .await
        .unwrap();
        assert_eq!(
            result,
            Posted::Posted {
                room: repo_room(),
                seq: RoomSeq(9)
            }
        );
    }

    #[tokio::test]
    async fn a_resolve_that_outlasts_the_call_deadline_is_timed_out_and_nothing_is_posted() {
        let mut daemon = FakeDaemon::start("pc", |request| match request {
            Request::Resolve { .. } => None,
            _ => Some(Response::Invalid {}),
        });
        let mut context = daemon.context(false);
        context.call_deadline = Duration::from_millis(200);
        assert_eq!(
            post(
                &context,
                &open(),
                Instant::now(),
                no_key,
                args(RoomScope::Repo, "x")
            )
            .await,
            Err(RoomFailure::TimedOut)
        );
        assert!(
            !daemon
                .requests()
                .iter()
                .any(|request| matches!(request, Request::Post { .. }))
        );
    }

    #[tokio::test]
    async fn the_drain_key_is_looked_up_only_after_the_body_and_the_cooldown_pass() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let mut daemon = fake(
            "pd",
            Response::Posted {
                room: repo_room(),
                seq: RoomSeq(1),
            },
        );
        let context = daemon.context(false);
        let gate = open();
        let looked = Arc::new(AtomicBool::new(false));
        let spy = |looked: &Arc<AtomicBool>| {
            let looked = looked.clone();
            move || {
                looked.store(true, Ordering::SeqCst);
                None
            }
        };
        let over = "x".repeat(MAX_BODY_CHARS + 1);
        let _ = post(
            &context,
            &gate,
            Instant::now(),
            spy(&looked),
            args(RoomScope::Repo, &over),
        )
        .await;
        assert!(
            !looked.load(Ordering::SeqCst),
            "looked up for a refused body"
        );
        let start = Instant::now();
        post(&context, &gate, start, no_key, args(RoomScope::Repo, "one"))
            .await
            .unwrap();
        let _ = post(
            &context,
            &gate,
            start,
            spy(&looked),
            args(RoomScope::Repo, "two"),
        )
        .await;
        assert!(
            !looked.load(Ordering::SeqCst),
            "looked up for a rate-limited call"
        );
        post(
            &context,
            &gate,
            start + COOLDOWN,
            spy(&looked),
            args(RoomScope::Repo, "three"),
        )
        .await
        .unwrap();
        assert!(looked.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_lookup_that_hangs_posts_without_a_key_instead_of_hanging() {
        let mut daemon = fake(
            "pe",
            Response::Posted {
                room: repo_room(),
                seq: RoomSeq(1),
            },
        );
        let mut context = daemon.context(false);
        context.lookup_deadline = Duration::from_millis(100);
        let started = std::time::Instant::now();
        let hangs = || {
            std::thread::sleep(Duration::from_millis(1500));
            Some(ConsumerKey::new("late").unwrap())
        };
        post(
            &context,
            &open(),
            Instant::now(),
            hangs,
            args(RoomScope::Repo, "x"),
        )
        .await
        .unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(matches!(
            daemon.requests().last(),
            Some(Request::Post { consumer: None, .. })
        ));
    }

    #[tokio::test]
    async fn the_tool_entry_point_without_a_config_is_config_unavailable() {
        assert_eq!(
            RoomPostTool::run(no_config(), args(RoomScope::Repo, "x")).await,
            Err(RoomFailure::ConfigUnavailable)
        );
    }
}
