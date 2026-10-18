//! The request protocol spoken over the daemon's local socket.
//!
//! One request per connection, one response, both newline-delimited JSON and
//! both capped in bytes before a parser sees them (rules 42, 44). The request
//! enum is closed and `deny_unknown_fields` at every level (rule 43): a local
//! client is a peer this daemon ships with, so a field it does not know means
//! something is wrong, not that the client is newer.
//!
//! Nothing here carries a credential. The daemon's token is used on the
//! daemon's own router connection and never crosses this socket, in either
//! direction. A message body travels only as a chat-wire [`Body`], so a raw
//! string cannot be posted (rules 9, 45).
//!
//! A [`ConsumerKey`] is a local label, for example a Claude Code session id,
//! that keeps one drain cursor per room. It is never sent to the router, and
//! nothing treats it as an identity: any process running as this user can
//! name any key, which is fine, because every such process is already this
//! user.

use std::fmt;

use jiff::Timestamp;
use sealmesh_chat_wire::{Body, MAX_BODY_CHARS, MessageKind, RoomId};
use sealmesh_proto::{BackfillRefusal, HistoryEntry, PostRefusal, RoomList, RoomSeq};
use sealmesh_wire::{HostId, RepoSlug, TicketId};
use serde::{Deserialize, Serialize};

/// The version of this protocol, reported by `status`. A client built against
/// a different one is talking to a daemon started from another build, and
/// the fix is restarting the daemon.
pub const PROTOCOL: u32 = 1;

/// The most bytes one request may take, newline included. A post carries at
/// most `MAX_BODY_CHARS` characters (16,000 bytes of UTF-8), which JSON can
/// escape to six bytes each at worst; everything else in a request is short.
pub const MAX_REQUEST_BYTES: usize = 64 * 1024;

/// The most bytes one response may take, newline included. Sized to hold one
/// router backfill frame (`MAX_FRAME_BYTES`, 512 KiB) with room to spare,
/// since re-encoding what the router sent never makes it meaningfully larger.
pub const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// What a drain or a tail may fill with messages: the response cap less room
/// for the envelope around them.
pub const MESSAGE_BUDGET_BYTES: usize = MAX_RESPONSE_BYTES - 64 * 1024;

/// The most bytes one delivered message can encode to in a response: a body
/// of `MAX_BODY_CHARS` characters escaped to six bytes each at worst, plus
/// its room, sender, kind, sequence and stamp, with room to spare. A test
/// encodes the worst case against it.
pub const MAX_DELIVERED_BYTES: usize = 6 * MAX_BODY_CHARS + 2 * 1024;

/// The most messages one drain or tail hands out.
pub const MAX_MESSAGES_PER_READ: u16 = 64;

/// The longest a tail may wait for something to arrive.
pub const MAX_TAIL_WAIT_MS: u32 = 10_000;

/// The longest a consumer key may be.
pub const MAX_CONSUMER_KEY_CHARS: usize = 128;

/// A local drain cursor's owner: a Claude Code session id, or any other label
/// a local tool picks. 1–128 characters of `[A-Za-z0-9._:-]`.
///
/// Its `Deserialize` goes through [`ConsumerKey::new`] (rule 45).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ConsumerKey(String);

impl ConsumerKey {
    pub fn new(value: &str) -> Result<Self, LocalError> {
        let ok = (1..=MAX_CONSUMER_KEY_CHARS).contains(&value.len())
            && value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-'));
        if ok {
            Ok(Self(value.to_owned()))
        } else {
            Err(LocalError::ConsumerKey)
        }
    }

    /// The drain key of a Claude Code session: its raw `sessionId`, whole —
    /// never the collector's eight-character short id. Every local consumer
    /// that speaks for a Claude Code session derives its key here, so the MCP
    /// server's posts (which read the id from the session file) and the
    /// hook's drain (which reads it from the hook payload's `session_id`)
    /// name the same key, and an agent's own posts are not drained back to
    /// it. `None` for an id that is not a valid key.
    pub fn from_session_id(session_id: &str) -> Option<Self> {
        Self::new(session_id).ok()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ConsumerKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConsumerKey({})", self.0)
    }
}

impl TryFrom<String> for ConsumerKey {
    type Error = LocalError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(&value)
    }
}

impl From<ConsumerKey> for String {
    fn from(key: ConsumerKey) -> Self {
        key.0
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LocalError {
    #[error("a consumer key must be 1-{MAX_CONSUMER_KEY_CHARS} chars of [A-Za-z0-9._:-]")]
    ConsumerKey,
    #[error("the message is {0} bytes, over the limit")]
    TooLarge(usize),
    #[error("the message is not valid json for this protocol (line {line}, column {column})")]
    Json { line: usize, column: usize },
}

/// A refusal category as a newer router may send it: one this build knows,
/// or one it does not.
///
/// Laptops upgrade by hand, so a router can be a version ahead of the daemon
/// talking to it. An unknown category is still a refusal, and dropping the
/// frame as undecodable would lose it silently, so the daemon's decode path
/// keeps it as `Unknown` (see `client::classify`). Encodes as the known
/// category's own word, or `"unknown"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Category<T> {
    Known(T),
    Unknown(UnknownCategory),
}

impl<T> Category<T> {
    pub const UNKNOWN: Self = Self::Unknown(UnknownCategory::Unknown);

    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown(_))
    }
}

/// The one word an unknown category encodes as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownCategory {
    Unknown,
}

pub type PostCategory = Category<PostRefusal>;
pub type BackfillCategory = Category<BackfillRefusal>;

/// Where a tail left off. `generation` names one life of a room in this
/// daemon's inbox — leaving and rejoining, or restarting the daemon, starts a
/// new one — so a cursor from an earlier life is recognised rather than read
/// against numbering that has restarted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TailCursor {
    pub generation: u64,
    pub ordinal: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    /// What this daemon holds: connection, rooms, inbox counts and losses.
    Status {},
    /// Join a room, answered once the router has (or at once, if this daemon
    /// already holds it).
    ///
    /// `holder` names whose claim this is: a bounded local label such as
    /// `chat:1234`, in the same shape as [`ConsumerKey`]. `None` is an
    /// anonymous, permanent claim — what an MCP tool's `join` sends, and
    /// what an older client (built before `holder` existed) always sends,
    /// since the field is optional. A room stays held as long as any claim on
    /// it does, named or anonymous (XYZ-65 follow-up: `chat` used to leave a
    /// room it did not join alone by never distinguishing itself). A named
    /// claim past `inbox::MAX_NAMED_CLAIMS` distinct holders on one room is
    /// answered `busy` rather than growing that room's bookkeeping without
    /// bound; it is never evicted to make room for a new one.
    Join {
        room: RoomId,
        #[serde(default)]
        holder: Option<ConsumerKey>,
    },
    /// Leaves one claim on a room: `holder: None` clears the
    /// anonymous/permanent slot, `holder: Some(key)` clears that one named
    /// claim, and either also clears the lease of the same key (see
    /// [`Request::Hold`]). The room is actually left — its undrained messages dropped
    /// and the router told — only once *no* claim remains; another holder's
    /// claim, named or anonymous, keeps it. `holder: None` is not a
    /// separate unconditional leave: nothing in this tree sends an untagged
    /// `Leave` today, so this is the only meaning it has ever had.
    Leave {
        room: RoomId,
        #[serde(default)]
        holder: Option<ConsumerKey>,
    },
    /// Post one message to a held room. `consumer`, when given, is the
    /// poster's own drain key, so its own message is not drained back to it.
    Post {
        room: RoomId,
        kind: MessageKind,
        body: Body,
        #[serde(default)]
        consumer: Option<ConsumerKey>,
    },
    /// Read a held room's stored history after `after`, one router frame's
    /// worth. Does not touch the inbox or any cursor.
    Backfill {
        room: RoomId,
        #[serde(default)]
        after: Option<RoomSeq>,
    },
    /// Hand `consumer` the messages it has not yet been handed, oldest first,
    /// and move its cursor past them. Each message goes to each consumer once.
    ///
    /// `max_messages` is clamped to 1..=`MAX_MESSAGES_PER_READ` and `max_bytes`
    /// to 1..=`MESSAGE_BUDGET_BYTES`, so a caller with a small per-turn cap
    /// (the hook) can ask for a few KiB and get no more. The first waiting
    /// message is always handed out, even alone over `max_bytes`, so no cap
    /// can wedge a consumer behind one message. The drain stops at the first
    /// message that does not fit rather than skipping it. Whatever the caps
    /// leave is not consumed, and the answer says `more`.
    Drain {
        consumer: ConsumerKey,
        #[serde(default)]
        room: Option<RoomId>,
        #[serde(default)]
        max_messages: Option<u16>,
        #[serde(default)]
        max_bytes: Option<u32>,
    },
    /// Read one room's inbox after `after` without consuming anything, waiting
    /// up to `wait_ms` for something new. For watching a room.
    Tail {
        room: RoomId,
        #[serde(default)]
        after: Option<TailCursor>,
        #[serde(default)]
        wait_ms: Option<u32>,
    },
    /// The live rooms this machine's credential may read. On demand only, and
    /// at most once per cooldown: each listing spends GitHub checks from the
    /// same allowance this host's `who` uses.
    ListRooms {},
    /// The rooms a checkout belongs to: the room of `ticket`, when given, and
    /// the room of `repo`, both in `repo`'s project, and that project's own
    /// room when `[chat] project_rooms` is on. Answered from the project map
    /// the router sent this daemon on connecting, never by asking it again
    /// per request. With `join`, each room is then joined through the same
    /// path as `join`, so the router decides each one.
    ///
    /// The caller reads `repo` and `ticket` from its own checkout
    /// (`gitref::read`); no path crosses this socket.
    ///
    /// A room joined this way is held under a lease, not a permanent claim:
    /// each `resolve` with `join` that names it again renews it, and one not
    /// renewed for `client::LEASE_IDLE` is let go. The lease names no holder;
    /// [`Request::Hold`] is the same with one.
    Resolve {
        repo: RepoSlug,
        #[serde(default)]
        ticket: Option<TicketId>,
        #[serde(default)]
        join: bool,
    },
    /// A `resolve` with `join`, each room held under `holder`'s own lease —
    /// for example the hook, which names its session on every prompt. The
    /// lease is renewed by asking again, let go once idle for
    /// `client::LEASE_IDLE`, and released at once by a `leave` naming the
    /// same `holder`. Answered `resolved`, as `resolve` is.
    ///
    /// A new request rather than a field on `resolve`, so a `resolve` still
    /// reads the same to a daemon of any age. A daemon built before `hold`
    /// answers it `invalid`; a caller that sees that may fall back to
    /// `resolve` with `join`, whose lease names no holder.
    Hold {
        repo: RepoSlug,
        #[serde(default)]
        ticket: Option<TicketId>,
        holder: ConsumerKey,
    },
}

/// How one room's join went, as part of a `resolve` with `join`. The words
/// mirror the responses a `join` request is answered with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinOutcome {
    Joined,
    /// Says nothing about why, as the router says nothing (rule 25).
    Refused,
    RoomLimit,
    Busy,
    TimedOut,
    NotConnected,
    Disabled,
}

impl JoinOutcome {
    /// The outcome a `join` response stands for. A response a join is never
    /// answered with reads as `refused`: not joined, and no more said.
    pub fn of(response: &Response) -> Self {
        match response {
            Response::Joined { .. } => Self::Joined,
            Response::RoomLimit {} => Self::RoomLimit,
            Response::Busy {} => Self::Busy,
            Response::TimedOut {} => Self::TimedOut,
            Response::NotConnected {} => Self::NotConnected,
            Response::Disabled {} => Self::Disabled,
            _ => Self::Refused,
        }
    }
}

/// One room a checkout belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedRoom {
    /// Spelled as the router's map spells it, which is the spelling a join
    /// is answered with.
    pub room: RoomId,
    /// Only when the request asked to join.
    #[serde(default)]
    pub join: Option<JoinOutcome>,
}

/// One message as the inbox hands it out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Delivered {
    pub room: RoomId,
    pub seq: RoomSeq,
    pub from: HostId,
    pub kind: MessageKind,
    pub body: Body,
    pub received_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoomStatus {
    pub room: RoomId,
    /// Joined on the current connection. False while a remembered room waits
    /// to be re-joined after a reconnect.
    pub held: bool,
    /// Messages waiting in this room's inbox.
    pub messages: usize,
    /// Messages this room's inbox dropped, oldest first, to stay in bounds.
    pub dropped: u64,
    pub last_refusal: Option<PostCategory>,
    pub turn_capped: bool,
    /// Whether a caller that names no holder holds this room, by a permanent
    /// claim (an MCP tool's `join`) or a lease (a `resolve` with `join`).
    /// `#[serde(default)]` so a client built after this field exists still
    /// decodes a daemon built before it did.
    #[serde(default)]
    pub anonymous_claim: bool,
    /// How many distinct named holders (for example a `chat` invocation, or
    /// a session's `hold`) currently claim this room, by a claim or a lease:
    /// at most `inbox::MAX_NAMED_CLAIMS` plus `inbox::MAX_LEASES`.
    #[serde(default)]
    pub named_claims: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Status {
    pub protocol: u32,
    pub connected: bool,
    pub host: Option<HostId>,
    pub rooms: Vec<RoomStatus>,
    /// Messages dropped across every room, including rooms since left.
    pub dropped: u64,
    /// Refusals whose category this build could not name.
    pub unknown_refusals: u64,
    pub consumers: usize,
    /// Consumers forgotten to stay under the cap, least recently used first.
    pub consumers_evicted: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum Response {
    /// Chat is off: `[chat] enabled` is false, or the kill file exists.
    Disabled {},
    /// The daemon has no router connection right now.
    NotConnected {},
    /// Too much is already in flight. Ask again shortly.
    Busy {},
    /// The router did not answer in time. An older router never answers
    /// room requests at all.
    TimedOut {},
    /// The request was too large or did not decode. Nothing of it is echoed.
    Invalid {},
    /// The answer would not fit in one response.
    TooLarge {},
    Status {
        status: Status,
    },
    Joined {
        room: RoomId,
    },
    /// Not joined. Says nothing about why, as the router says nothing
    /// (rule 25).
    JoinRefused {
        room: RoomId,
    },
    /// Already at the most rooms this daemon holds.
    RoomLimit {},
    Left {
        room: RoomId,
    },
    /// This daemon does not hold that room. Join it first.
    NotMember {
        room: RoomId,
    },
    /// The router recorded and delivered the post as `seq`.
    Posted {
        room: RoomId,
        seq: RoomSeq,
    },
    PostRefused {
        room: RoomId,
        category: PostCategory,
    },
    /// The post was sent, but neither its delivery nor a refusal came back in
    /// time. It may or may not have been delivered.
    PostUnconfirmed {
        room: RoomId,
    },
    Backfill {
        room: RoomId,
        after: Option<RoomSeq>,
        messages: Vec<HistoryEntry>,
        next: Option<RoomSeq>,
    },
    BackfillRefused {
        room: RoomId,
        category: BackfillCategory,
    },
    /// A drain's result. `lost` counts messages the inbox dropped before this
    /// consumer was handed them. `new_consumer` means this key had no cursor —
    /// first use, or forgotten under the cap — and was started ten minutes
    /// back; anything older is reachable through `backfill`. `more` means the
    /// caps stopped the drain before the inbox was empty for this consumer.
    Messages {
        messages: Vec<Delivered>,
        lost: u64,
        new_consumer: bool,
        more: bool,
    },
    /// A tail's result. Pass `cursor` back as `after` to continue. `reset`
    /// means the cursor passed in was from an earlier life of the room and
    /// reading started over from the oldest message held.
    Tail {
        room: RoomId,
        messages: Vec<Delivered>,
        cursor: TailCursor,
        lost: u64,
        reset: bool,
    },
    /// The live rooms this machine may read. `age_secs` is how old the listing
    /// is: within the cooldown, the previous listing is answered again rather
    /// than spending more GitHub checks.
    Rooms {
        rooms: RoomList,
        age_secs: u64,
    },
    /// The router refused to list because a listing was already running.
    RoomListRefused {},
    /// A listing was asked for within the cooldown and none is held to
    /// answer with.
    ListThrottled {
        retry_after_secs: u64,
    },
    /// A checkout's rooms, ticket room first, then repo, then project. Empty
    /// when its repo is in no project, when it is in one whose repos this
    /// machine's token cannot read, and when the router sent no map (an
    /// older router never does) — one answer for all three, so it reveals
    /// nothing about a project this machine has no repo in (rule 25).
    Resolved {
        rooms: Vec<ResolvedRoom>,
    },
}

/// Decodes one request or response, refusing it before parsing when it is
/// over `limit` bytes. An error names a position, never the bytes (rule 14).
pub fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8], limit: usize) -> Result<T, LocalError> {
    if bytes.len() > limit {
        return Err(LocalError::TooLarge(bytes.len()));
    }
    serde_json::from_slice(bytes).map_err(|e| LocalError::Json {
        line: e.line(),
        column: e.column(),
    })
}

/// Encodes one line, newline included, refusing one over `limit` bytes.
pub fn encode<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>, LocalError> {
    let mut line =
        serde_json::to_vec(value).map_err(|_| LocalError::Json { line: 0, column: 0 })?;
    line.push(b'\n');
    if line.len() > limit {
        return Err(LocalError::TooLarge(line.len()));
    }
    Ok(line)
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use sealmesh_chat_wire::{MAX_BODY_CHARS, Scope};
    use sealmesh_wire::ProjectId;

    use super::*;

    fn room() -> RoomId {
        RoomId::new(
            ProjectId::try_from("example-repo").unwrap(),
            Scope::Repo(RepoSlug::try_from("example-org/example-repo").unwrap()),
        )
    }

    #[test]
    fn every_request_round_trips() {
        let requests = [
            Request::Status {},
            Request::Join {
                room: room(),
                holder: None,
            },
            Request::Join {
                room: room(),
                holder: Some(ConsumerKey::new("chat:1234").unwrap()),
            },
            Request::Leave {
                room: room(),
                holder: None,
            },
            Request::Leave {
                room: room(),
                holder: Some(ConsumerKey::new("chat:1234").unwrap()),
            },
            Request::Post {
                room: room(),
                kind: MessageKind::Claim,
                body: Body::new("taking the portal").unwrap(),
                consumer: Some(ConsumerKey::new("abc-123").unwrap()),
            },
            Request::Backfill {
                room: room(),
                after: Some(RoomSeq(4)),
            },
            Request::Drain {
                consumer: ConsumerKey::new("abc-123").unwrap(),
                room: None,
                max_messages: Some(3),
                max_bytes: None,
            },
            Request::Tail {
                room: room(),
                after: Some(TailCursor {
                    generation: 1,
                    ordinal: 2,
                }),
                wait_ms: Some(10),
            },
            Request::ListRooms {},
            Request::Resolve {
                repo: RepoSlug::try_from("example-org/example-repo").unwrap(),
                ticket: Some(TicketId::new("ENG", 7).unwrap()),
                join: true,
            },
        ];
        for request in requests {
            let line = encode(&request, MAX_REQUEST_BYTES).unwrap();
            let back: Request = decode(line.trim_ascii_end(), MAX_REQUEST_BYTES).unwrap();
            assert_eq!(back, request);
        }
    }

    #[test]
    fn an_unknown_operation_or_field_is_refused() {
        for hostile in [
            r#"{"op":"shell","cmd":"ls"}"#,
            r#"{"op":"status","token":"ghp_x"}"#,
            r#"{"op":"list_rooms","extra":1}"#,
            r#"{"op":"drain","consumer":"a","as_host":"shelter"}"#,
            r#"{"op":"post","room":{"project":"example-repo","scope":"project"},"kind":"claim","body":"x","from":"shelter"}"#,
            r#"{"op":"post","room":{"project":"example-repo","scope":"project"},"kind":"shout","body":"x"}"#,
        ] {
            assert!(
                decode::<Request>(hostile.as_bytes(), MAX_REQUEST_BYTES).is_err(),
                "accepted {hostile}"
            );
        }
    }

    #[test]
    fn a_resolve_goes_through_the_validating_newtypes_and_nothing_else() {
        let ok = r#"{"op":"resolve","repo":"example-org/example-repo"}"#;
        assert_eq!(
            decode::<Request>(ok.as_bytes(), MAX_REQUEST_BYTES).unwrap(),
            Request::Resolve {
                repo: RepoSlug::try_from("example-org/example-repo").unwrap(),
                ticket: None,
                join: false,
            }
        );
        for hostile in [
            r#"{"op":"resolve","repo":"a/b/c"}"#,
            r#"{"op":"resolve","repo":"example-org/example-repo","ticket":"not a ticket"}"#,
            r#"{"op":"resolve","repo":"example-org/example-repo","path":"/home/x/work"}"#,
            r#"{"op":"resolve","repo":"example-org/example-repo","project":"example-repo"}"#,
            r#"{"op":"resolve"}"#,
        ] {
            assert!(
                decode::<Request>(hostile.as_bytes(), MAX_REQUEST_BYTES).is_err(),
                "accepted {hostile}"
            );
        }
        let answer = Response::Resolved {
            rooms: vec![ResolvedRoom {
                room: room(),
                join: Some(JoinOutcome::Refused),
            }],
        };
        let line = encode(&answer, MAX_RESPONSE_BYTES).unwrap();
        assert_eq!(
            decode::<Response>(line.trim_ascii_end(), MAX_RESPONSE_BYTES).unwrap(),
            answer
        );
        let extra = r#"{"result":"resolved","rooms":[],"project":"example-repo"}"#;
        assert!(decode::<Response>(extra.as_bytes(), MAX_RESPONSE_BYTES).is_err());
    }

    #[test]
    fn the_existing_requests_still_decode_as_they_did() {
        // A client built before `resolve` sends exactly these.
        for line in [
            r#"{"op":"status"}"#,
            r#"{"op":"list_rooms"}"#,
            r#"{"op":"drain","consumer":"a","max_bytes":131072}"#,
        ] {
            assert!(
                decode::<Request>(line.as_bytes(), MAX_REQUEST_BYTES).is_ok(),
                "{line}"
            );
        }
        assert_eq!(PROTOCOL, 1);
    }

    #[test]
    fn a_join_or_leave_from_before_holder_existed_still_decodes_as_an_anonymous_claim() {
        // A client built before XYZ-65's follow-up sends exactly these, with
        // no `holder` field at all — additive, so `PROTOCOL` does not change.
        let join = r#"{"op":"join","room":{"project":"example-repo","scope":{"repo":"example-org/example-repo"}}}"#;
        assert_eq!(
            decode::<Request>(join.as_bytes(), MAX_REQUEST_BYTES).unwrap(),
            Request::Join {
                room: room(),
                holder: None,
            }
        );
        let leave = r#"{"op":"leave","room":{"project":"example-repo","scope":{"repo":"example-org/example-repo"}}}"#;
        assert_eq!(
            decode::<Request>(leave.as_bytes(), MAX_REQUEST_BYTES).unwrap(),
            Request::Leave {
                room: room(),
                holder: None,
            }
        );
        assert_eq!(PROTOCOL, 1);
    }

    #[test]
    fn a_body_goes_through_the_capped_newtype_on_the_way_in() {
        let long = "x".repeat(MAX_BODY_CHARS + 1);
        let json = serde_json::json!({
            "op": "post",
            "room": room(),
            "kind": "claim",
            "body": long,
        })
        .to_string();
        assert!(decode::<Request>(json.as_bytes(), MAX_REQUEST_BYTES).is_err());

        let empty = serde_json::json!({"op":"post","room":room(),"kind":"claim","body":""});
        assert!(decode::<Request>(empty.to_string().as_bytes(), MAX_REQUEST_BYTES).is_err());
    }

    #[test]
    fn the_largest_post_fits_the_request_cap_even_fully_escaped() {
        // Every character a control character: the worst case for JSON's
        // escaping, six bytes each.
        let body = Body::new("\u{1}".repeat(MAX_BODY_CHARS)).unwrap();
        let request = Request::Post {
            room: RoomId::new(
                ProjectId::try_from("a".repeat(32).as_str()).unwrap(),
                Scope::Repo(
                    RepoSlug::try_from(format!("{}/{}", "o".repeat(100), "r".repeat(100)).as_str())
                        .unwrap(),
                ),
            ),
            kind: MessageKind::Handoff,
            body,
            consumer: Some(ConsumerKey::new(&"k".repeat(MAX_CONSUMER_KEY_CHARS)).unwrap()),
        };
        let line = encode(&request, MAX_REQUEST_BYTES).unwrap();
        assert!(line.len() <= MAX_REQUEST_BYTES, "{}", line.len());
    }

    #[test]
    fn an_oversized_line_is_refused_before_it_is_parsed() {
        let big = vec![b' '; MAX_REQUEST_BYTES + 1];
        assert_eq!(
            decode::<Request>(&big, MAX_REQUEST_BYTES),
            Err(LocalError::TooLarge(MAX_REQUEST_BYTES + 1))
        );
    }

    #[test]
    fn a_decode_error_does_not_echo_what_it_choked_on() {
        let error = decode::<Request>(br#"{"op":"ghp_secretvalue"}"#, MAX_REQUEST_BYTES)
            .unwrap_err()
            .to_string();
        assert!(!error.contains("ghp_secretvalue"), "{error}");
    }

    #[test]
    fn a_consumer_key_is_bounded_and_charset_checked() {
        assert!(ConsumerKey::new("0b3e5c0a-6f7e-4a21-9d1c-5b7c1f0e2a11").is_ok());
        assert!(ConsumerKey::new(&"a".repeat(MAX_CONSUMER_KEY_CHARS)).is_ok());
        for bad in [
            String::new(),
            "a".repeat(MAX_CONSUMER_KEY_CHARS + 1),
            "../etc".to_owned(),
            "a b".to_owned(),
            "é".to_owned(),
            "a\n".to_owned(),
        ] {
            assert!(ConsumerKey::new(&bad).is_err(), "accepted {bad:?}");
            let json = serde_json::to_string(&bad).unwrap();
            assert!(serde_json::from_str::<ConsumerKey>(&json).is_err());
        }
    }

    #[test]
    fn a_claude_session_id_is_its_own_drain_key_whole() {
        let id = "0b3e5c0a-6f7e-4a21-9d1c-5b7c1f0e2a11";
        let key = ConsumerKey::from_session_id(id).unwrap();
        assert_eq!(key.as_str(), id);
        assert_eq!(ConsumerKey::from_session_id("../etc"), None);
    }

    #[test]
    fn a_category_encodes_as_its_word_or_unknown() {
        let known: PostCategory = Category::Known(PostRefusal::RateLimited);
        assert_eq!(serde_json::to_string(&known).unwrap(), r#""rate_limited""#);
        let unknown: PostCategory = Category::UNKNOWN;
        assert_eq!(serde_json::to_string(&unknown).unwrap(), r#""unknown""#);
        assert_eq!(
            serde_json::from_str::<PostCategory>(r#""unknown""#).unwrap(),
            unknown
        );
        assert_eq!(
            serde_json::from_str::<BackfillCategory>(r#""busy""#).unwrap(),
            Category::Known(BackfillRefusal::Busy)
        );
        assert!(serde_json::from_str::<PostCategory>(r#""made_up""#).is_err());
    }

    #[test]
    fn a_response_refuses_an_unknown_field() {
        assert!(decode::<Response>(br#"{"result":"busy","why":"x"}"#, MAX_RESPONSE_BYTES).is_err());
        assert!(decode::<Response>(br#"{"result":"busy"}"#, MAX_RESPONSE_BYTES).is_ok());
    }

    #[test]
    fn the_message_budget_leaves_room_for_the_envelope() {
        const { assert!(MAX_DELIVERED_BYTES <= MESSAGE_BUDGET_BYTES) };
        const { assert!(MESSAGE_BUDGET_BYTES < MAX_RESPONSE_BYTES) };
    }

    /// The largest message this protocol can carry, encoded: every body
    /// character a control character (six bytes escaped), the longest project,
    /// repo and host. It must fit `MAX_DELIVERED_BYTES`, which a stored
    /// message falls back to costing when it cannot be measured.
    #[test]
    fn the_largest_message_fits_the_drain_floor() {
        // A `Body` of this build replaces a control character, but
        // a daemon built before that can still send one, so the body is put
        // in as a JSON value: the older peer's worst case.
        let body = Body::new("x").unwrap();
        let delivered = Delivered {
            room: RoomId::new(
                ProjectId::try_from("a".repeat(32).as_str()).unwrap(),
                Scope::Repo(
                    RepoSlug::try_from(format!("{}/{}", "o".repeat(100), "r".repeat(100)).as_str())
                        .unwrap(),
                ),
            ),
            seq: RoomSeq(u64::MAX),
            from: longest_host(),
            kind: MessageKind::Handoff,
            body,
            received_at: Timestamp::MAX,
        };
        let mut delivered = serde_json::to_value(delivered).unwrap();
        *delivered.get_mut("body").unwrap() =
            serde_json::Value::String("\u{1}".repeat(MAX_BODY_CHARS));
        let line = encode(&delivered, usize::MAX).unwrap();
        assert!(line.len() <= MAX_DELIVERED_BYTES, "{}", line.len());
        // And the floor is not wastefully far above it.
        assert!(
            MAX_DELIVERED_BYTES - line.len() < 4 * 1024,
            "{}",
            line.len()
        );
    }

    fn longest_host() -> HostId {
        // The longest name `HostId` accepts, found rather than assumed.
        let mut longest = HostId::try_from("a").unwrap();
        for n in 1..=1024 {
            match HostId::try_from("a".repeat(n).as_str()) {
                Ok(host) => longest = host,
                Err(_) => break,
            }
        }
        longest
    }
}
