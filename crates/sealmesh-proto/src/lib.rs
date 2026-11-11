use std::fmt;

pub mod tailnet;

use sealmesh_chat_wire::{Body, MessageKind, RoomId};
use sealmesh_wire::{Event, HostId, ProjectId, RepoSlug, Snapshot};
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretToken(String);

impl SecretToken {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretToken(redacted)")
    }
}

pub const MAX_FRAME_BYTES: usize = 512 * 1024;

pub const MAX_EVENTS_PER_BATCH: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<Event>", into = "Vec<Event>")]
pub struct EventBatch(Vec<Event>);

impl EventBatch {
    pub fn new(events: Vec<Event>) -> Result<Self, ProtoError> {
        if events.len() > MAX_EVENTS_PER_BATCH {
            return Err(ProtoError::TooManyEvents(events.len()));
        }
        Ok(Self(events))
    }

    pub fn as_slice(&self) -> &[Event] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<Event> {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<EventBatch> for Vec<Event> {
    fn from(batch: EventBatch) -> Self {
        batch.0
    }
}

impl TryFrom<Vec<Event>> for EventBatch {
    type Error = ProtoError;

    fn try_from(events: Vec<Event>) -> Result<Self, Self::Error> {
        Self::new(events)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct QueryId(pub u64);

/// A message's position in one room, assigned by the router when it fans the
/// message out.
///
/// Only the router issues one. No client frame carries a sequence number —
/// `ClientFrame::Post` is `deny_unknown_fields`, so a post that names its own
/// `seq` does not decode at all — which makes a forged sequence number
/// something a peer cannot even express, the way a daemon can only answer a
/// `QueryId` the registry issued.
///
/// Counted per room, and continued from the room's stored history rather than
/// restarted: a room that empties is forgotten by the registry, but when it
/// re-forms the router reads the highest sequence number that room has ever
/// persisted and carries on from there. The same is true after a deploy. So a
/// sequence number identifies a message within a room over time, not only
/// within one of its lives.
///
/// Monotonic within a room, but **not** guaranteed to be gap-free. A number is
/// taken before the message is written, so a post refused because it could not
/// be recorded leaves its number unused. A reader must order by `seq` and must
/// not treat a missing number as a message it failed to receive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RoomSeq(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Daemon,
    Reader,
}

/// One project's slice of the router's repo→project mapping, as it travels on
/// the wire. Shaped exactly like the server's own `RawProject` because there
/// is nothing to add: `id` and `repos` already go through their validating
/// constructors before this type ever sees them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectEntry {
    pub id: ProjectId,
    pub repos: Vec<RepoSlug>,
}

/// Longest a machine name may be: one DNS label.
pub const MAX_MACHINE_NAME: usize = 63;

/// Which of one person's machines a daemon connection comes from.
///
/// A host is a GitHub login, so two laptops signed in as the same person are
/// one host. The router keeps one connection per machine, and this name is
/// how it tells them apart: a connection from a machine it already holds
/// replaces that one, and a connection from another machine is added beside
/// it.
///
/// Sent by the daemon, so it is a label and never a credential. It cannot
/// reach anyone else's rows — the host it is attached to is still the one the
/// daemon's own token resolved to (rule 12) — and every limit stays keyed by
/// the host, so naming more machines buys nothing.
///
/// 1–63 characters of `[a-z0-9-]`, starting alphanumeric: the `HostId` rule
/// at the length of one DNS label, because it is usually a hostname and it
/// renders in a terminal beside one. A `Deserialize` that routes through the
/// same check (rule 45).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MachineName(String);

impl MachineName {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The machine name an OS hostname reads as: its first label, lowercased,
    /// with every run of characters outside `[a-z0-9]` folded to one `-`, and
    /// no `-` at either end. `None` when nothing is left.
    ///
    /// `Santiagos-MacBook-Pro.local` becomes `santiagos-macbook-pro`.
    pub fn from_hostname(hostname: &str) -> Option<Self> {
        let label = hostname.split('.').next().unwrap_or_default();
        let mut name = String::with_capacity(label.len().min(MAX_MACHINE_NAME));
        for c in label.chars() {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_lowercase() || c.is_ascii_digit() {
                name.push(c);
            } else if !name.is_empty() && !name.ends_with('-') {
                name.push('-');
            }
            if name.len() >= MAX_MACHINE_NAME {
                break;
            }
        }
        while name.ends_with('-') {
            name.pop();
        }
        Self::try_from(name.as_str()).ok()
    }
}

impl fmt::Display for MachineName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<MachineName> for String {
    fn from(name: MachineName) -> Self {
        name.0
    }
}

impl TryFrom<&str> for MachineName {
    type Error = ProtoError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let ok = (1..=MAX_MACHINE_NAME).contains(&value.len())
            && value.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
            && value
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
        if ok {
            Ok(Self(value.to_owned()))
        } else {
            Err(ProtoError::MachineName)
        }
    }
}

impl TryFrom<String> for MachineName {
    type Error = ProtoError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServerFrame {
    Welcome {
        host: HostId,
    },
    Query {
        id: QueryId,
        asked_by: HostId,
    },
    Report {
        id: QueryId,
        host: HostId,
        state: HostState,
    },
    Complete {
        id: QueryId,
    },
    /// Answers a `ClientFrame::Projects` request. A new frame variant rather
    /// than a field on `Welcome`, because `Welcome` is `deny_unknown_fields`
    /// and every reader already installed would refuse a `Welcome` carrying a
    /// field it does not know. A router that predates this variant never
    /// sends it, so an old reader talking to a new router is unaffected, and
    /// a new reader talking to an old router simply never gets a reply — see
    /// `ClientFrame::Projects`.
    Projects {
        projects: Vec<ProjectEntry>,
    },
    /// Sent once, before `Complete`, when the router withheld part of the
    /// answer to query `id`: work in a repo the reader's own token cannot
    /// read, or one the router could not confirm it can read.
    ///
    /// It says only that the answer is partial. No count, no repo, no host —
    /// "how much" and "where" are exactly what the reader is not permitted to
    /// learn (rule 25), so there is nothing else for this frame to carry.
    ///
    /// A new variant rather than a field on `Complete`, because `Complete` is
    /// `deny_unknown_fields`: every installed reader would refuse a `Complete`
    /// carrying a field it does not know, and a reader that never decodes
    /// `Complete` never stops waiting. An installed reader instead fails to
    /// decode this frame, counts it as one answer it could not read, keeps
    /// reading, and still returns on the `Complete` that follows — printing
    /// its existing "could not be read by this build … Upgrade sealmesh"
    /// note, which is true.
    Partial {
        id: QueryId,
    },
    /// `{}` rather than a bare unit variant for the reason on
    /// `ClientFrame::Projects`, and all the more because a refusal must not
    /// say which check failed (rule 25). It encodes as `{"type":"denied"}`,
    /// byte for byte what the unit variant encoded, so peers on either side of
    /// the change still decode each other.
    Denied {},
    /// Answers `ClientFrame::Join`: the daemon is now a member of `room`.
    ///
    /// `room` is spelled as the router's project map spells it, which can
    /// differ in case from the repo the daemon asked for. That spelling is the
    /// room's one key, so two daemons that spell a repo differently still land
    /// in one room.
    ///
    /// Every room frame is a new variant rather than a field on an existing
    /// frame: every frame is `deny_unknown_fields`, so a field would break
    /// every installed peer, while a variant an old peer does not know is one
    /// frame it skips as undecodable.
    Joined {
        room: RoomId,
    },
    /// Answers `ClientFrame::Join`: not joined. Carries the room as the daemon
    /// sent it and nothing else — a project the router has never heard of, a
    /// repo the caller cannot read, a GitHub outage, a full room and a join
    /// rate limit are all this one frame (rule 25).
    JoinRefused {
        room: RoomId,
    },
    /// The daemon is no longer a member of `room`: it asked to leave, or its
    /// credential no longer passes the join check when the router re-checks.
    Left {
        room: RoomId,
    },
    /// A message posted to a room this daemon is a member of.
    ///
    /// `from` is the host the poster's credential resolved to at its
    /// handshake, never a value any client sent (rule 12), and `seq` is the
    /// router's. `body` is prose, capped by `Body`, and its `Debug` is
    /// redacted so this frame cannot put a message on a log line.
    Message {
        room: RoomId,
        seq: RoomSeq,
        from: HostId,
        kind: MessageKind,
        body: Body,
    },
    /// Answers a `ClientFrame::Post` the router refused. The post was not
    /// delivered to anyone, and it used no sequence number.
    ///
    /// `room` is spelled the way the router keys it, as in `Joined`. The
    /// poster is a member, so that spelling tells it nothing new.
    /// `category` says which limit refused the post, and nothing else rides
    /// along: no body, no count, no retry time. That is enough for an agent
    /// to tell "slow down" from "stop for a while". Neither limit reveals
    /// anything about access, so rule 25 does not require collapsing them.
    ///
    /// Sent for every refused post, not just the first, so an agent never
    /// has to guess whether it is muted. A post from a connection that is not
    /// in the room is still dropped with no answer.
    ///
    /// A new variant rather than a field on an existing frame, for the reason
    /// on `Joined`. A daemon built before this variant cannot decode it,
    /// skips it as undecodable and keeps its socket. A daemon built before
    /// this variant never posts, either, so it never has a post refused.
    PostRefused {
        room: RoomId,
        category: PostRefusal,
    },
    /// Tells this daemon that the exchange it is in with one other host in
    /// `room` has reached the router's turn cap. From now on its own posts to
    /// that room are refused as `PostRefusal::TurnCapped`, until a third host
    /// posts there or an hour passes with neither participant trying to.
    ///
    /// The other participant's post is the one that hit the cap, and that
    /// post was answered with `PostRefused`. This frame is how the side that
    /// did not post finds out, so both agents see the stop. Sent once per
    /// capped exchange, not once per refused post, so a retrying participant
    /// cannot flood the other.
    ///
    /// Carries the room and nothing else: no count, no other host, no prose.
    /// The daemon already knows who it was talking to from `Message::from`.
    ///
    /// A new variant rather than a field on an existing frame, for the reason
    /// on `Joined`. A daemon built before it skips it as undecodable and keeps
    /// its socket.
    TurnCapped {
        room: RoomId,
    },
    /// Answers `ClientFrame::Backfill`: part of `room`'s stored history, oldest
    /// first, starting after `after`.
    ///
    /// `room` is the router's key for the room, as in `Joined`; `after` is the
    /// cursor the request carried, echoed so a client that stopped waiting on
    /// one request can tell a late answer to it from the answer to the next.
    /// `next` is where to resume: `Some` when there may be more, `None` when
    /// this reply reached the end of what the room has stored. A reply can
    /// hold fewer messages than the room has and still carry `next`, because
    /// it is sized to fit one frame. An empty `messages` with `next: None` is
    /// a room with nothing stored after `after`.
    ///
    /// Sequence numbers are monotonic but not gap-free (see `RoomSeq`), so a
    /// missing number between two entries is not a message this reply lost.
    ///
    /// A new variant rather than a field on an existing frame, for the reason
    /// on `Joined`. A daemon built before it never asks for one.
    Backfill {
        room: RoomId,
        after: Option<RoomSeq>,
        messages: Vec<HistoryEntry>,
        next: Option<RoomSeq>,
    },
    /// Answers `ClientFrame::Backfill`: nothing was read. Carries the room as
    /// the daemon sent it and a category.
    ///
    /// A room this connection does not hold is one category, `not_held`,
    /// whether the room is someone else's, has history but no members, or has
    /// never existed (rule 25) — and it is decided before the store is asked
    /// anything, so how long the answer takes says nothing either.
    BackfillRefused {
        room: RoomId,
        category: BackfillRefusal,
    },
    /// Answers `ClientFrame::ListRooms`: the live rooms this connection's own
    /// credential passes the join check for, at most `MAX_LISTED_ROOMS` of
    /// them.
    ///
    /// A live room is one that has members right now. A room with stored
    /// history and nobody in it is not listed, and is still read by joining it
    /// and asking for a backfill. A room the join check refused, could not
    /// finish, or had no allowance left to ask GitHub about is left out, and
    /// nothing here says one was: an omission reveals nothing, and a partial
    /// list is the fail-closed direction (rule 25).
    RoomList {
        rooms: RoomList,
    },
    /// Answers `ClientFrame::ListRooms` that arrived while this connection's
    /// previous listing was still running. Nothing was checked. Ask again once
    /// the first has its answer.
    ///
    /// `{}` rather than a bare unit variant, for the reason on
    /// `ClientFrame::Projects`.
    RoomListRefused {},
    /// Answers `ClientFrame::AskMachines`: one connected machine's answer,
    /// exactly as `Report` carries it, plus which of the host's machines it
    /// came from.
    ///
    /// `machine` is `None` for a daemon that never named one — a build from
    /// before machine names — and for a host with nothing connected. One host
    /// can have several of these per query, one per machine. `host` is still
    /// the router's attribution, and the snapshot inside `state` still names
    /// that host, so the reader's attribution check is unchanged (rule 21).
    ///
    /// A new variant rather than a field on `Report`, for the reason on
    /// `Joined`. Only a reader that asked with `AskMachines` gets one, and a
    /// reader built before it never asks.
    MachineReport {
        id: QueryId,
        host: HostId,
        machine: Option<MachineName>,
        state: HostState,
    },
}

/// One stored room message, as a backfill carries it: the same four fields
/// as `ServerFrame::Message` without the room, which the reply names once.
///
/// `from` is the host the poster's credential resolved to, as it was when the
/// message was recorded, and `seq` is the router's. `body` is prose, capped by
/// `Body`, with a redacted `Debug`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryEntry {
    pub seq: RoomSeq,
    pub from: HostId,
    pub kind: MessageKind,
    pub body: Body,
}

/// Why a backfill read nothing. A closed set: an unknown category does not
/// decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackfillRefusal {
    /// This connection does not hold the room. One answer for a room that is
    /// someone else's, one with history and no members, and one that has never
    /// existed (rule 25). Join it, then ask again.
    NotHeld,
    /// This connection's previous backfill is still being read. Ask again once
    /// it has its answer.
    Busy,
    /// The room is held, but its history could not be read: the store was
    /// unreachable or slow, or held an item this build will not trust. Worth
    /// retrying. Never carries anything the store returned (rules 14, 26).
    Unavailable,
}

/// Most rooms one `ServerFrame::RoomList` may carry.
pub const MAX_LISTED_ROOMS: usize = 64;

/// The rooms a listing carries, never more than [`MAX_LISTED_ROOMS`].
///
/// A newtype whose `Deserialize` routes through [`RoomList::new`] (rule 45),
/// so a longer list cannot be built by the router or decoded by a daemon.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(try_from = "Vec<RoomId>", into = "Vec<RoomId>")]
pub struct RoomList(Vec<RoomId>);

impl RoomList {
    pub fn new(rooms: Vec<RoomId>) -> Result<Self, ProtoError> {
        if rooms.len() > MAX_LISTED_ROOMS {
            return Err(ProtoError::TooManyRooms(rooms.len()));
        }
        Ok(Self(rooms))
    }

    pub fn as_slice(&self) -> &[RoomId] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<RoomList> for Vec<RoomId> {
    fn from(list: RoomList) -> Self {
        list.0
    }
}

impl TryFrom<Vec<RoomId>> for RoomList {
    type Error = ProtoError;

    fn try_from(rooms: Vec<RoomId>) -> Result<Self, Self::Error> {
        Self::new(rooms)
    }
}

/// Which limit refused a post. A closed set: an unknown category does not
/// decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostRefusal {
    /// This host is posting faster than the router's post rate allows, across
    /// every connection it has made.
    /// It recovers within seconds.
    RateLimited,
    /// This host has spent its token allowance for this room. The allowance
    /// refills continuously, and posting to another room is unaffected.
    BudgetExhausted,
    /// This host and one other have taken the most turns the router's turn cap
    /// allows in this room, with no third host posting. A turn is one or more
    /// posts in a row by one host. Both are refused
    /// in this room until a third host posts, or until an hour passes with
    /// neither of them trying to post there. Retrying does not lift it: every
    /// refused attempt restarts that hour. Neither does leaving and rejoining.
    /// Posting to another room is unaffected.
    ///
    /// A daemon built before this category cannot decode a `PostRefused`
    /// carrying it, and skips that frame as undecodable.
    TurnCapped,
    /// The router could not record this message, so it delivered it to nobody.
    ///
    /// A room's messages are written to history before they are fanned out, so
    /// that a room never delivers a message its history will not have. When
    /// the write cannot be made, the post is refused rather than delivered and
    /// silently lost from history: an agent can retry a refusal, and cannot
    /// discover a gap.
    ///
    /// Unlike the other three, this says nothing about the poster's own
    /// behaviour — it is not a limit, and slowing down does not help. It is
    /// worth retrying, and worth telling a human about if it persists.
    ///
    /// A daemon built before this category cannot decode a `PostRefused`
    /// carrying it, and skips that frame as undecodable.
    NotRecorded,
    /// The room is a ticket room whose ticket's pull request has merged, so it
    /// is closed to writes. Its history is still readable, and joining it
    /// still works; nothing is deleted, and the history's 7-day retention is
    /// what removes it.
    ///
    /// The merge is what the router confirmed with GitHub, looked up from the
    /// ticket and the room's project — never a pull request number a client
    /// named. It is final for this room: retrying, leaving and rejoining do
    /// not lift it. Posting to another room is unaffected.
    ///
    /// A daemon built before this category cannot decode a `PostRefused`
    /// carrying it, and skips that frame as undecodable.
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientFrame {
    Hello {
        role: Role,
        token: SecretToken,
    },
    Ask {
        id: QueryId,
    },
    Answer {
        id: QueryId,
        state: HostState,
    },
    Events {
        batch: EventBatch,
    },
    /// Asks the router for its repo→project mapping. A router built before
    /// this shipped does not have this variant in its own `ClientFrame` enum,
    /// so it fails to decode the frame and — per the existing
    /// `Received::Undecodable` handling — simply ignores it rather than
    /// answering. The reader must bound how long it waits for a
    /// `ServerFrame::Projects` reply and fall back to ungrouped output rather
    /// than hang.
    ///
    /// `{}` rather than a bare unit variant: serde's internally tagged enums
    /// do not run `deny_unknown_fields` for a fieldless unit variant, so a
    /// bare `Projects` would silently accept `{"type":"projects","x":1}`. An
    /// explicit empty struct variant goes through the same field-checking
    /// path every other variant does.
    Projects {},
    /// Asks to join `room`, answered with `ServerFrame::Joined` or
    /// `ServerFrame::JoinRefused`. Honoured only from a daemon, and only after
    /// its handshake: before `Welcome` the only frame a router reads is
    /// `Hello`.
    ///
    /// A router built before rooms cannot decode this and ignores it, so a
    /// daemon must bound how long it waits for the answer.
    ///
    /// **Wait for the answer before sending the next `Join`, and before
    /// posting to this room.** The router runs one access check per connection
    /// at a time, so a `Join` that arrives while another is still being
    /// checked is refused as `JoinRefused` rather than queued — ask again once
    /// the first has its answer. A `Post` to this room sent before `Joined`
    /// arrives is dropped, because the connection is not a member yet. A
    /// `Leave` for this room sent before the answer cancels the join, which is
    /// then answered `JoinRefused` and never admits.
    ///
    /// Joining a room this connection already holds is idempotent: it is
    /// answered `Joined` at once, with no new access check and without counting
    /// against the join rate limit. A `Leave` after it is an ordinary leave,
    /// answered `Left`. Losing access to a held room is still caught by the
    /// router's periodic re-check.
    Join {
        room: RoomId,
    },
    /// Leaves `room`, answered with `ServerFrame::Left`. Naming a room this
    /// connection is not in is dropped with no answer.
    Leave {
        room: RoomId,
    },
    /// Posts a message to `room`. Delivered only when this connection is a
    /// member of it; otherwise dropped with no answer, like an `Answer` for a
    /// `QueryId` the router never issued (rule 20).
    ///
    /// A member's post can still be refused by the router's post rate or by
    /// this host's token allowance for the room. That is answered with
    /// `ServerFrame::PostRefused`, and nothing is delivered.
    ///
    /// Carries no sender and no sequence number: the router supplies both.
    ///
    /// **Post to a room only after `Joined` for it has arrived.** A `Post`
    /// sent while its `Join` is still being checked is dropped, like any post
    /// to a room the connection is not in, and nothing says so.
    Post {
        room: RoomId,
        kind: MessageKind,
        body: Body,
    },
    /// Asks for `room`'s stored history after `after` (from the start when
    /// `None`), answered with `ServerFrame::Backfill` or
    /// `ServerFrame::BackfillRefused`. Honoured only from a daemon, and only
    /// for a room this connection holds — join it first. An archived ticket
    /// room is still held and still readable.
    ///
    /// One answer per request, sized to fit one frame. Follow its `next` with
    /// another request until `next` is `None`.
    ///
    /// **Wait for the answer before sending the next `Backfill`.** One read
    /// runs per connection at a time, and a second that arrives meanwhile is
    /// refused as `busy` rather than queued.
    ///
    /// A router built before this cannot decode it and ignores it, so a daemon
    /// must bound how long it waits for the answer.
    Backfill {
        room: RoomId,
        after: Option<RoomSeq>,
    },
    /// Asks which live rooms this connection may read, answered with
    /// `ServerFrame::RoomList` or `ServerFrame::RoomListRefused`. Honoured
    /// only from a daemon.
    ///
    /// **Wait for the answer before asking again**: a second listing while one
    /// is running is refused rather than queued.
    ///
    /// A router built before this cannot decode it and ignores it, so a daemon
    /// must bound how long it waits for the answer. `{}` rather than a bare
    /// unit variant, for the reason on `Projects`.
    ListRooms {},
    /// Names the machine this daemon connection comes from. Sent once, right
    /// after `Welcome`; a second one on the same connection is ignored.
    ///
    /// The router keeps one connection per machine of a host. A connection
    /// that names a machine the host already has connected replaces that
    /// connection, which is how a laptop waking from sleep takes its old,
    /// dead socket's place. A connection that never sends this — a daemon
    /// built before it — keeps a slot of its own until its socket closes.
    ///
    /// A router built before this cannot decode it and ignores it, keeping
    /// the socket, and then treats every connection of a host as the same
    /// machine, as it always has.
    Machine {
        name: MachineName,
    },
    /// `Ask`, answered with one `ServerFrame::MachineReport` per machine
    /// instead of one `Report`, so a reader can show which of a host's
    /// machines each answer came from. Then `Partial` if anything was
    /// withheld and `Complete`, exactly as for `Ask`.
    ///
    /// A router built before this cannot decode it and ignores it, so it never
    /// answers. A reader that sends this sends `Projects` right behind it: a
    /// router that answers `Projects` first skipped this request, and the
    /// reader asks again with `Ask`.
    AskMachines {
        id: QueryId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum HostState {
    Answering {
        snapshot: Box<Snapshot>,
    },
    /// `{}` rather than a bare unit variant for the reason on
    /// `ClientFrame::Projects`. It encodes as `{"state":"not_sharing"}`, byte
    /// for byte what the unit variant encoded, so peers on either side of the
    /// change still decode each other.
    NotSharing {},
    /// `{}` for the same reason as `NotSharing`, and encoded as
    /// `{"state":"unreachable"}` exactly as before.
    Unreachable {},
    /// Connected and sharing, but refusing this query because it has been
    /// asked more often than its `QueryBudget` answers. Kept apart from
    /// `Unreachable` because a looping caller draining the budget otherwise
    /// makes a working machine look like a closed laptop to every reader.
    ///
    /// It carries nothing: no reason, no retry time, no count. `{}` rather
    /// than a bare unit variant for the reason on `ClientFrame::Projects`:
    /// serde does not check fields on an internally tagged unit variant.
    ///
    /// A peer built before this variant cannot decode a frame carrying it.
    /// An old router counts the `Answer` as undecodable and keeps the socket,
    /// so the ticket times out to `Unreachable`. An old reader counts the
    /// `Report` as undecodable, keeps reading and still completes. Neither
    /// closes a connection or loses another machine's row.
    Throttled {},
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProtoError {
    #[error("frame is {0} bytes, over the limit")]
    TooLarge(usize),
    #[error("frame carries {0} events, over the limit")]
    TooManyEvents(usize),
    #[error("room list carries {0} rooms, over the limit")]
    TooManyRooms(usize),
    #[error("machine name must be 1-63 chars of [a-z0-9-], starting alphanumeric")]
    MachineName,
    #[error("frame is not valid json for this protocol (line {line}, column {column})")]
    Json { line: usize, column: usize },
}

pub fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, ProtoError> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(ProtoError::TooLarge(bytes.len()));
    }
    serde_json::from_slice(bytes).map_err(|e| ProtoError::Json {
        line: e.line(),
        column: e.column(),
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::collections::BTreeSet;

    use super::*;

    fn host() -> HostId {
        HostId::try_from("sandbank").unwrap()
    }

    #[test]
    fn frames_round_trip() {
        let frames = [
            ServerFrame::Welcome { host: host() },
            ServerFrame::Query {
                id: QueryId(1),
                asked_by: host(),
            },
            ServerFrame::Complete { id: QueryId(1) },
            ServerFrame::Denied {},
        ];
        for frame in frames {
            let json = serde_json::to_vec(&frame).unwrap();
            assert_eq!(decode::<ServerFrame>(&json).unwrap(), frame);
        }
    }

    #[test]
    fn the_host_states_are_distinguishable_on_the_wire() {
        let states = [
            HostState::NotSharing {},
            HostState::Unreachable {},
            HostState::Throttled {},
        ];
        for state in &states {
            let json = serde_json::to_vec(state).unwrap();
            let back: HostState = decode(&json).unwrap();
            assert_eq!(&back, state);
        }
        let encoded: BTreeSet<String> = states
            .iter()
            .map(|state| serde_json::to_string(state).unwrap())
            .collect();
        assert_eq!(
            encoded.len(),
            states.len(),
            "an opt-out, a dead laptop and a refusal must not serialise alike: {encoded:?}"
        );
    }

    #[test]
    fn a_throttled_state_is_a_bare_tag_and_refuses_anything_riding_along() {
        let json = serde_json::to_string(&HostState::Throttled {}).unwrap();
        assert_eq!(json, r#"{"state":"throttled"}"#);
        assert!(decode::<HostState>(br#"{"state":"throttled","retry_after":6}"#).is_err());
        assert!(decode::<HostState>(br#"{"state":"throttled","reason":"busy"}"#).is_err());
    }

    /// `HostState` exactly as every peer built before XYZ-92 encodes and
    /// decodes it, with `NotSharing` and `Unreachable` as bare unit variants.
    #[allow(dead_code)]
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
    enum HostStateWithUnitVariants {
        Answering { snapshot: Box<Snapshot> },
        NotSharing,
        Unreachable,
        Throttled {},
    }

    #[test]
    fn a_not_sharing_state_refuses_anything_riding_along() {
        assert!(decode::<HostState>(br#"{"state":"not_sharing"}"#).is_ok());
        assert!(decode::<HostState>(br#"{"state":"not_sharing","x":1}"#).is_err());
        assert!(decode::<HostState>(br#"{"state":"not_sharing","reason":"off"}"#).is_err());
    }

    #[test]
    fn an_unreachable_state_refuses_anything_riding_along() {
        assert!(decode::<HostState>(br#"{"state":"unreachable"}"#).is_ok());
        assert!(decode::<HostState>(br#"{"state":"unreachable","x":1}"#).is_err());
        assert!(decode::<HostState>(br#"{"state":"unreachable","since":6}"#).is_err());
    }

    /// Pins the defect the `{}` variants fix: serde skips the field check on
    /// an internally tagged unit variant, so the old shape lets a stray field
    /// through. Without this, the two refusal tests above could pass for some
    /// reason other than the variant shape.
    #[test]
    fn a_bare_unit_variant_is_what_let_a_stray_field_through() {
        assert!(decode::<HostStateWithUnitVariants>(br#"{"state":"not_sharing","x":1}"#).is_ok());
        assert!(decode::<HostStateWithUnitVariants>(br#"{"state":"unreachable","x":1}"#).is_ok());
    }

    #[test]
    fn the_empty_variants_encode_exactly_as_the_unit_variants_did_in_both_directions() {
        let cases = [
            (
                HostState::NotSharing {},
                HostStateWithUnitVariants::NotSharing,
                r#"{"state":"not_sharing"}"#,
            ),
            (
                HostState::Unreachable {},
                HostStateWithUnitVariants::Unreachable,
                r#"{"state":"unreachable"}"#,
            ),
        ];
        for (new, old, bytes) in cases {
            let new_encoded = serde_json::to_string(&new).unwrap();
            let old_encoded = serde_json::to_string(&old).unwrap();
            assert_eq!(new_encoded, bytes);
            assert_eq!(old_encoded, bytes);

            // An installed peer decodes what this build sends...
            assert_eq!(
                decode::<HostStateWithUnitVariants>(new_encoded.as_bytes()).unwrap(),
                old
            );
            // ...and this build decodes what an installed peer sends.
            assert_eq!(decode::<HostState>(old_encoded.as_bytes()).unwrap(), new);
        }
    }

    #[test]
    fn a_denied_frame_is_a_bare_tag_in_both_directions_and_refuses_anything_riding_along() {
        let new_encoded = serde_json::to_string(&ServerFrame::Denied {}).unwrap();
        let old_encoded = serde_json::to_string(&ServerFrameBeforePartial::Denied).unwrap();
        assert_eq!(new_encoded, r#"{"type":"denied"}"#);
        assert_eq!(old_encoded, new_encoded);
        assert!(matches!(
            decode::<ServerFrameBeforePartial>(new_encoded.as_bytes()),
            Ok(ServerFrameBeforePartial::Denied)
        ));
        assert_eq!(
            decode::<ServerFrame>(old_encoded.as_bytes()).unwrap(),
            ServerFrame::Denied {}
        );

        // A refusal must not say which check failed (rule 25), so nothing
        // may ride along with one.
        assert!(decode::<ServerFrame>(br#"{"type":"denied","reason":"not_in_org"}"#).is_err());
        assert!(decode::<ServerFrame>(br#"{"type":"denied","x":1}"#).is_err());
    }

    /// `HostState` exactly as a router or reader built before `Throttled`
    /// knows it.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
    enum HostStateBeforeThrottled {
        Answering { snapshot: Box<Snapshot> },
        NotSharing,
        Unreachable,
    }

    /// `ClientFrame` as a router built before `Throttled` knows it.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    enum ClientFrameBeforeThrottled {
        Hello {
            role: Role,
            token: SecretToken,
        },
        Ask {
            id: QueryId,
        },
        Answer {
            id: QueryId,
            state: HostStateBeforeThrottled,
        },
        Events {
            batch: EventBatch,
        },
        Projects {},
    }

    /// `ServerFrame` as a reader built before `Throttled` knows it.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    enum ServerFrameBeforeThrottled {
        Welcome {
            host: HostId,
        },
        Query {
            id: QueryId,
            asked_by: HostId,
        },
        Report {
            id: QueryId,
            host: HostId,
            state: HostStateBeforeThrottled,
        },
        Complete {
            id: QueryId,
        },
        Projects {
            projects: Vec<ProjectEntry>,
        },
        Partial {
            id: QueryId,
        },
        Denied,
    }

    #[test]
    fn a_router_built_before_throttled_cannot_decode_the_answer_but_still_reads_the_next() {
        // The old router classifies a frame it cannot decode as undecodable,
        // keeps the socket, and lets that host's ticket time out to
        // `Unreachable`. So the new answer must fail to decode there, and an
        // ordinary answer after it must still decode.
        let throttled = serde_json::to_vec(&ClientFrame::Answer {
            id: QueryId(1),
            state: HostState::Throttled {},
        })
        .unwrap();
        assert!(decode::<ClientFrameBeforeThrottled>(&throttled).is_err());

        let next = serde_json::to_vec(&ClientFrame::Answer {
            id: QueryId(2),
            state: HostState::Unreachable {},
        })
        .unwrap();
        assert!(matches!(
            decode::<ClientFrameBeforeThrottled>(&next),
            Ok(ClientFrameBeforeThrottled::Answer {
                id: QueryId(2),
                state: HostStateBeforeThrottled::Unreachable
            })
        ));
    }

    #[test]
    fn a_reader_built_before_throttled_skips_the_report_and_still_sees_the_rest() {
        let throttled = serde_json::to_vec(&ServerFrame::Report {
            id: QueryId(1),
            host: host(),
            state: HostState::Throttled {},
        })
        .unwrap();
        assert!(decode::<ServerFrameBeforeThrottled>(&throttled).is_err());

        let other = serde_json::to_vec(&ServerFrame::Report {
            id: QueryId(1),
            host: HostId::try_from("shelter").unwrap(),
            state: HostState::NotSharing {},
        })
        .unwrap();
        assert!(matches!(
            decode::<ServerFrameBeforeThrottled>(&other),
            Ok(ServerFrameBeforeThrottled::Report {
                state: HostStateBeforeThrottled::NotSharing,
                ..
            })
        ));

        let complete = serde_json::to_vec(&ServerFrame::Complete { id: QueryId(1) }).unwrap();
        assert!(matches!(
            decode::<ServerFrameBeforeThrottled>(&complete),
            Ok(ServerFrameBeforeThrottled::Complete { id: QueryId(1) })
        ));
    }

    #[test]
    fn an_unknown_field_is_refused() {
        let json = br#"{"type":"welcome","host":"sandbank","extra":1}"#;
        assert!(decode::<ServerFrame>(json).is_err());
    }

    #[test]
    fn a_partial_notice_round_trips_and_carries_nothing_but_the_query() {
        let frame = ServerFrame::Partial { id: QueryId(4) };
        let json = serde_json::to_string(&frame).unwrap();
        assert_eq!(json, r#"{"type":"partial","id":4}"#);
        assert_eq!(decode::<ServerFrame>(json.as_bytes()).unwrap(), frame);

        // Nothing can ride along — a count or a repo would be exactly what
        // rule 25 forbids this frame from saying.
        assert!(decode::<ServerFrame>(br#"{"type":"partial","id":4,"withheld":3}"#).is_err());
    }

    /// `ServerFrame` exactly as an installed reader built before `Partial`
    /// knows it.
    #[allow(dead_code)]
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    enum ServerFrameBeforePartial {
        Welcome {
            host: HostId,
        },
        Query {
            id: QueryId,
            asked_by: HostId,
        },
        Report {
            id: QueryId,
            host: HostId,
            state: HostState,
        },
        Complete {
            id: QueryId,
        },
        Projects {
            projects: Vec<ProjectEntry>,
        },
        Denied,
    }

    #[test]
    fn a_reader_built_before_partial_skips_it_and_still_sees_complete() {
        // An installed `who` classifies a frame it cannot decode as
        // undecodable and keeps reading. So the partial notice must fail to
        // decode there — not decode as something else — and the `Complete`
        // behind it must still decode, or that reader waits forever.
        let partial = serde_json::to_vec(&ServerFrame::Partial { id: QueryId(1) }).unwrap();
        assert!(decode::<ServerFrameBeforePartial>(&partial).is_err());

        let complete = serde_json::to_vec(&ServerFrame::Complete { id: QueryId(1) }).unwrap();
        assert!(matches!(
            decode::<ServerFrameBeforePartial>(&complete),
            Ok(ServerFrameBeforePartial::Complete { id: QueryId(1) })
        ));
    }

    fn project(id: &str, repos: &[&str]) -> ProjectEntry {
        ProjectEntry {
            id: ProjectId::try_from(id).unwrap(),
            repos: repos
                .iter()
                .map(|r| RepoSlug::try_from(*r).unwrap())
                .collect(),
        }
    }

    #[test]
    fn a_projects_request_and_reply_round_trip() {
        let request = ClientFrame::Projects {};
        let json = serde_json::to_vec(&request).unwrap();
        assert_eq!(decode::<ClientFrame>(&json).unwrap(), request);

        let reply = ServerFrame::Projects {
            projects: vec![
                project(
                    "example-repo",
                    &[
                        "example-org/example-repo",
                        "example-org/example-repo-portal",
                    ],
                ),
                project("example-tools", &["example-org/example-tools"]),
            ],
        };
        let json = serde_json::to_vec(&reply).unwrap();
        assert_eq!(decode::<ServerFrame>(&json).unwrap(), reply);
    }

    #[test]
    fn an_empty_projects_reply_is_a_legal_answer() {
        let reply = ServerFrame::Projects { projects: vec![] };
        let json = serde_json::to_vec(&reply).unwrap();
        assert_eq!(decode::<ServerFrame>(&json).unwrap(), reply);
    }

    #[test]
    fn a_projects_request_carries_no_fields_to_smuggle_one_in() {
        let json = br#"{"type":"projects","extra":1}"#;
        assert!(decode::<ClientFrame>(json).is_err());
    }

    #[test]
    fn a_projects_reply_refuses_an_unknown_field_on_one_entry() {
        let json =
            br#"{"type":"projects","projects":[{"id":"example-repo","repos":[],"owner":"x"}]}"#;
        assert!(decode::<ServerFrame>(json).is_err());
    }

    #[test]
    fn a_projects_reply_refuses_an_unknown_top_level_field() {
        let json = br#"{"type":"projects","projects":[],"x":1}"#;
        assert!(decode::<ServerFrame>(json).is_err());
    }

    #[test]
    fn an_old_router_never_sending_projects_is_distinguishable_from_an_empty_one() {
        // `ServerFrame::Projects { projects: vec![] }` (a router with no
        // mapping configured) and "no reply at all" (an old router that does
        // not know this frame) are different situations on the wire, even
        // though a reader that times out waiting for the second one falls
        // back to the same ungrouped rendering as the first. This pins both
        // halves of that claim: the empty reply is a real frame that
        // round-trips, and "no bytes at all" -- the wire-level stand-in for
        // a router that never replies -- is not the same outcome; it fails
        // to decode instead.
        let reply = ServerFrame::Projects { projects: vec![] };
        let json = serde_json::to_vec(&reply).unwrap();
        assert_eq!(decode::<ServerFrame>(&json).unwrap(), reply);
        assert!(decode::<ServerFrame>(b"").is_err());
    }

    #[test]
    fn a_token_is_redacted_by_debug_and_absent_from_a_frame_dump() {
        let token = SecretToken::new("ghp_realsecretvalue".to_owned());
        assert_eq!(format!("{token:?}"), "SecretToken(redacted)");

        let frame = ClientFrame::Hello {
            role: Role::Daemon,
            token,
        };
        let dumped = format!("{frame:?}");
        assert!(!dumped.contains("ghp_realsecretvalue"), "{dumped}");
    }

    #[test]
    fn a_token_round_trips_as_a_plain_string() {
        let frame = ClientFrame::Hello {
            role: Role::Reader,
            token: SecretToken::new("ghp_x".to_owned()),
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains(r#""token":"ghp_x""#), "{json}");
        assert_eq!(decode::<ClientFrame>(json.as_bytes()).unwrap(), frame);
    }

    fn event() -> Event {
        use sealmesh_wire::{EVENT_SCHEMA_VERSION, EventKind, JobId, RepoSlug};

        Event {
            schema: EVENT_SCHEMA_VERSION,
            at: "2026-08-10T19:56:51.322Z".parse().unwrap(),
            engineer: host(),
            repo: RepoSlug::try_from("example-org/example-repo").unwrap(),
            job: JobId::try_from("1e631e34").unwrap(),
            kind: EventKind::Started,
            ticket: None,
            branch: None,
            pr: None,
        }
    }

    #[test]
    fn a_batch_larger_than_the_cap_cannot_be_held_or_decoded() {
        let too_many = vec![event(); MAX_EVENTS_PER_BATCH + 1];
        assert_eq!(
            EventBatch::new(too_many.clone()),
            Err(ProtoError::TooManyEvents(MAX_EVENTS_PER_BATCH + 1))
        );

        let json = serde_json::to_vec(&serde_json::json!({
            "type": "events",
            "batch": too_many,
        }))
        .unwrap();
        assert!(decode::<ClientFrame>(&json).is_err());

        assert!(EventBatch::new(vec![event(); MAX_EVENTS_PER_BATCH]).is_ok());
    }

    #[test]
    fn an_events_frame_round_trips() {
        let frame = ClientFrame::Events {
            batch: EventBatch::new(vec![event()]).unwrap(),
        };
        let json = serde_json::to_vec(&frame).unwrap();
        assert_eq!(decode::<ClientFrame>(&json).unwrap(), frame);
    }

    #[test]
    fn a_batch_is_a_plain_array_on_the_wire() {
        let batch = EventBatch::new(vec![event()]).unwrap();
        let value = serde_json::to_value(&batch).unwrap();
        assert!(value.is_array(), "{value}");
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn an_oversized_frame_is_refused_before_parsing() {
        let big = vec![b'a'; MAX_FRAME_BYTES + 1];
        assert_eq!(
            decode::<ServerFrame>(&big),
            Err(ProtoError::TooLarge(MAX_FRAME_BYTES + 1))
        );
    }

    fn room() -> RoomId {
        RoomId::new(
            ProjectId::try_from("example-repo").unwrap(),
            sealmesh_chat_wire::Scope::Repo(
                RepoSlug::try_from("example-org/example-repo").unwrap(),
            ),
        )
    }

    fn body(text: &str) -> Body {
        Body::new(text).unwrap()
    }

    fn room_client_frames() -> Vec<ClientFrame> {
        vec![
            ClientFrame::Join { room: room() },
            ClientFrame::Leave { room: room() },
            ClientFrame::Post {
                room: room(),
                kind: MessageKind::Claim,
                body: body("taking ENG-557"),
            },
        ]
    }

    fn room_server_frames() -> Vec<ServerFrame> {
        vec![
            ServerFrame::Joined { room: room() },
            ServerFrame::JoinRefused { room: room() },
            ServerFrame::Left { room: room() },
            ServerFrame::Message {
                room: room(),
                seq: RoomSeq(7),
                from: host(),
                kind: MessageKind::Question,
                body: body("who has the portal?"),
            },
            ServerFrame::PostRefused {
                room: room(),
                category: PostRefusal::RateLimited,
            },
            ServerFrame::PostRefused {
                room: room(),
                category: PostRefusal::BudgetExhausted,
            },
            ServerFrame::PostRefused {
                room: room(),
                category: PostRefusal::TurnCapped,
            },
            ServerFrame::PostRefused {
                room: room(),
                category: PostRefusal::NotRecorded,
            },
            ServerFrame::PostRefused {
                room: room(),
                category: PostRefusal::Archived,
            },
            ServerFrame::TurnCapped { room: room() },
        ]
    }

    #[test]
    fn a_post_refusal_carries_the_room_and_a_category_and_nothing_else() {
        let room_json = r#"{"project":"example-repo","scope":"project"}"#;
        for (category, word) in [
            (PostRefusal::RateLimited, "rate_limited"),
            (PostRefusal::BudgetExhausted, "budget_exhausted"),
            (PostRefusal::TurnCapped, "turn_capped"),
            (PostRefusal::NotRecorded, "not_recorded"),
            (PostRefusal::Archived, "archived"),
        ] {
            let frame = ServerFrame::PostRefused {
                room: RoomId::new(
                    ProjectId::try_from("example-repo").unwrap(),
                    sealmesh_chat_wire::Scope::Project,
                ),
                category,
            };
            let json = serde_json::to_string(&frame).unwrap();
            assert_eq!(
                json,
                format!(r#"{{"type":"post_refused","room":{room_json},"category":"{word}"}}"#)
            );
            assert_eq!(decode::<ServerFrame>(json.as_bytes()).unwrap(), frame);
        }

        for hostile in [
            format!(
                r#"{{"type":"post_refused","room":{room_json},"category":"rate_limited","retry_after":2}}"#
            ),
            format!(
                r#"{{"type":"post_refused","room":{room_json},"category":"rate_limited","body":"x"}}"#
            ),
            format!(r#"{{"type":"post_refused","room":{room_json},"category":"muted_forever"}}"#),
            format!(r#"{{"type":"post_refused","room":{room_json}}}"#),
        ] {
            assert!(
                decode::<ServerFrame>(hostile.as_bytes()).is_err(),
                "accepted {hostile}"
            );
        }
    }

    /// `ServerFrame` exactly as a daemon built after rooms and before
    /// `PostRefused` knows it.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    enum ServerFrameBeforePostRefused {
        Welcome {
            host: HostId,
        },
        Query {
            id: QueryId,
            asked_by: HostId,
        },
        Report {
            id: QueryId,
            host: HostId,
            state: HostState,
        },
        Complete {
            id: QueryId,
        },
        Projects {
            projects: Vec<ProjectEntry>,
        },
        Partial {
            id: QueryId,
        },
        Denied {},
        Joined {
            room: RoomId,
        },
        JoinRefused {
            room: RoomId,
        },
        Left {
            room: RoomId,
        },
        Message {
            room: RoomId,
            seq: RoomSeq,
            from: HostId,
            kind: MessageKind,
            body: Body,
        },
    }

    #[test]
    fn a_daemon_built_before_post_refused_skips_it_and_still_reads_the_next_frame() {
        // An installed daemon classifies a frame it cannot decode as
        // undecodable and keeps its socket. So the refusal must fail to
        // decode there, not decode as something else, and the frames around
        // it must still decode.
        for category in [PostRefusal::RateLimited, PostRefusal::BudgetExhausted] {
            let refused = serde_json::to_vec(&ServerFrame::PostRefused {
                room: room(),
                category,
            })
            .unwrap();
            assert!(decode::<ServerFrameBeforePostRefused>(&refused).is_err());
        }
        for frame in [
            ServerFrame::Message {
                room: room(),
                seq: RoomSeq(2),
                from: host(),
                kind: MessageKind::Ack,
                body: body("ok"),
            },
            ServerFrame::Query {
                id: QueryId(9),
                asked_by: host(),
            },
        ] {
            let json = serde_json::to_vec(&frame).unwrap();
            assert!(
                decode::<ServerFrameBeforePostRefused>(&json).is_ok(),
                "{frame:?}"
            );
        }
    }

    /// `PostRefusal` exactly as a daemon built after `PostRefused` and before
    /// the turn cap knows it.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum PostRefusalBeforeTurnCap {
        RateLimited,
        BudgetExhausted,
    }

    /// `ServerFrame` exactly as a daemon built after `PostRefused` and before
    /// the turn cap knows it.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    enum ServerFrameBeforeTurnCap {
        Welcome {
            host: HostId,
        },
        Query {
            id: QueryId,
            asked_by: HostId,
        },
        Report {
            id: QueryId,
            host: HostId,
            state: HostState,
        },
        Complete {
            id: QueryId,
        },
        Projects {
            projects: Vec<ProjectEntry>,
        },
        Partial {
            id: QueryId,
        },
        Denied {},
        Joined {
            room: RoomId,
        },
        JoinRefused {
            room: RoomId,
        },
        Left {
            room: RoomId,
        },
        Message {
            room: RoomId,
            seq: RoomSeq,
            from: HostId,
            kind: MessageKind,
            body: Body,
        },
        PostRefused {
            room: RoomId,
            category: PostRefusalBeforeTurnCap,
        },
    }

    #[test]
    fn a_daemon_built_before_the_turn_cap_skips_its_frames_and_still_reads_the_next_one() {
        // An installed daemon classifies a frame it cannot decode as
        // undecodable and keeps its socket. So a turn-capped refusal and the
        // notice must fail to decode there, not decode as something else, and
        // the frames around them must still decode.
        for frame in [
            ServerFrame::PostRefused {
                room: room(),
                category: PostRefusal::TurnCapped,
            },
            ServerFrame::TurnCapped { room: room() },
        ] {
            let json = serde_json::to_vec(&frame).unwrap();
            assert!(
                decode::<ServerFrameBeforeTurnCap>(&json).is_err(),
                "{frame:?}"
            );
        }
        for frame in [
            ServerFrame::PostRefused {
                room: room(),
                category: PostRefusal::RateLimited,
            },
            ServerFrame::Message {
                room: room(),
                seq: RoomSeq(3),
                from: host(),
                kind: MessageKind::Ack,
                body: body("ok"),
            },
            ServerFrame::Query {
                id: QueryId(9),
                asked_by: host(),
            },
        ] {
            let json = serde_json::to_vec(&frame).unwrap();
            assert!(
                decode::<ServerFrameBeforeTurnCap>(&json).is_ok(),
                "{frame:?}"
            );
        }
    }

    /// `PostRefusal` exactly as a daemon built after the turn cap and before
    /// the history store knows it.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum PostRefusalBeforeNotRecorded {
        RateLimited,
        BudgetExhausted,
        TurnCapped,
    }

    /// `ServerFrame` exactly as that daemon knows it.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    enum ServerFrameBeforeNotRecorded {
        Welcome {
            host: HostId,
        },
        Query {
            id: QueryId,
            asked_by: HostId,
        },
        Report {
            id: QueryId,
            host: HostId,
            state: HostState,
        },
        Complete {
            id: QueryId,
        },
        Projects {
            projects: Vec<ProjectEntry>,
        },
        Partial {
            id: QueryId,
        },
        Denied {},
        Joined {
            room: RoomId,
        },
        JoinRefused {
            room: RoomId,
        },
        Left {
            room: RoomId,
        },
        Message {
            room: RoomId,
            seq: RoomSeq,
            from: HostId,
            kind: MessageKind,
            body: Body,
        },
        PostRefused {
            room: RoomId,
            category: PostRefusalBeforeNotRecorded,
        },
        TurnCapped {
            room: RoomId,
        },
    }

    #[test]
    fn a_daemon_built_before_not_recorded_skips_it_and_still_reads_the_next_frame() {
        // An installed daemon classifies a frame it cannot decode as
        // undecodable and keeps its socket. So a refusal carrying the new
        // category must fail to decode there, not decode as something else,
        // and every frame around it must still decode.
        let refused = serde_json::to_vec(&ServerFrame::PostRefused {
            room: room(),
            category: PostRefusal::NotRecorded,
        })
        .unwrap();
        assert!(decode::<ServerFrameBeforeNotRecorded>(&refused).is_err());

        for frame in [
            ServerFrame::PostRefused {
                room: room(),
                category: PostRefusal::TurnCapped,
            },
            ServerFrame::TurnCapped { room: room() },
            ServerFrame::Message {
                room: room(),
                seq: RoomSeq(4),
                from: host(),
                kind: MessageKind::Ack,
                body: body("ok"),
            },
        ] {
            let json = serde_json::to_vec(&frame).unwrap();
            assert!(
                decode::<ServerFrameBeforeNotRecorded>(&json).is_ok(),
                "{frame:?}"
            );
        }
    }

    /// `PostRefusal` exactly as a daemon built after the history store and
    /// before archive-on-merge knows it.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum PostRefusalBeforeArchived {
        RateLimited,
        BudgetExhausted,
        TurnCapped,
        NotRecorded,
    }

    /// `ServerFrame` exactly as that daemon knows it.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    enum ServerFrameBeforeArchived {
        Welcome {
            host: HostId,
        },
        Query {
            id: QueryId,
            asked_by: HostId,
        },
        Report {
            id: QueryId,
            host: HostId,
            state: HostState,
        },
        Complete {
            id: QueryId,
        },
        Projects {
            projects: Vec<ProjectEntry>,
        },
        Partial {
            id: QueryId,
        },
        Denied {},
        Joined {
            room: RoomId,
        },
        JoinRefused {
            room: RoomId,
        },
        Left {
            room: RoomId,
        },
        Message {
            room: RoomId,
            seq: RoomSeq,
            from: HostId,
            kind: MessageKind,
            body: Body,
        },
        PostRefused {
            room: RoomId,
            category: PostRefusalBeforeArchived,
        },
        TurnCapped {
            room: RoomId,
        },
    }

    #[test]
    fn a_daemon_built_before_archived_skips_it_and_still_reads_the_next_frame() {
        // The same property as `not_recorded`'s: an installed daemon that
        // cannot decode the new category must see an undecodable frame, which
        // it skips and keeps its socket, and every frame around it must still
        // decode.
        let refused = serde_json::to_vec(&ServerFrame::PostRefused {
            room: room(),
            category: PostRefusal::Archived,
        })
        .unwrap();
        assert!(decode::<ServerFrameBeforeArchived>(&refused).is_err());

        for frame in [
            ServerFrame::PostRefused {
                room: room(),
                category: PostRefusal::NotRecorded,
            },
            ServerFrame::TurnCapped { room: room() },
            ServerFrame::Message {
                room: room(),
                seq: RoomSeq(4),
                from: host(),
                kind: MessageKind::Ack,
                body: body("ok"),
            },
        ] {
            let json = serde_json::to_vec(&frame).unwrap();
            assert!(
                decode::<ServerFrameBeforeArchived>(&json).is_ok(),
                "{frame:?}"
            );
        }
    }

    #[test]
    fn a_post_cannot_name_the_pull_request_that_archives_its_room() {
        // Rule 12: the merge that archives a ticket room is looked up by the
        // router. A post that tries to assert one is not a post at all.
        let post = serde_json::to_value(ClientFrame::Post {
            room: room(),
            kind: MessageKind::Claim,
            body: body("merged"),
        })
        .unwrap();
        for (field, value) in [
            ("pr", serde_json::json!(12)),
            ("pull_request", serde_json::json!(12)),
            ("merged", serde_json::json!(true)),
            ("archive", serde_json::json!(true)),
        ] {
            let mut hostile = post.clone();
            hostile
                .as_object_mut()
                .unwrap()
                .insert(field.to_owned(), value);
            let json = serde_json::to_vec(&hostile).unwrap();
            assert!(decode::<ClientFrame>(&json).is_err(), "accepted {field}");
        }
    }

    #[test]
    fn a_turn_cap_notice_carries_the_room_and_nothing_else() {
        let room_json = r#"{"project":"example-repo","scope":"project"}"#;
        let frame = ServerFrame::TurnCapped {
            room: RoomId::new(
                ProjectId::try_from("example-repo").unwrap(),
                sealmesh_chat_wire::Scope::Project,
            ),
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert_eq!(
            json,
            format!(r#"{{"type":"turn_capped","room":{room_json}}}"#)
        );
        assert_eq!(decode::<ServerFrame>(json.as_bytes()).unwrap(), frame);
        for hostile in [
            format!(r#"{{"type":"turn_capped","room":{room_json},"with":"bay"}}"#),
            format!(r#"{{"type":"turn_capped","room":{room_json},"posts":20}}"#),
            r#"{"type":"turn_capped"}"#.to_owned(),
        ] {
            assert!(
                decode::<ServerFrame>(hostile.as_bytes()).is_err(),
                "accepted {hostile}"
            );
        }
    }

    #[test]
    fn room_frames_round_trip() {
        for frame in room_client_frames() {
            let json = serde_json::to_vec(&frame).unwrap();
            assert_eq!(decode::<ClientFrame>(&json).unwrap(), frame);
        }
        for frame in room_server_frames() {
            let json = serde_json::to_vec(&frame).unwrap();
            assert_eq!(decode::<ServerFrame>(&json).unwrap(), frame);
        }
    }

    #[test]
    fn a_post_cannot_name_its_own_sequence_number_or_sender() {
        let room = r#"{"project":"example-repo","scope":"project"}"#;
        let valid = format!(r#"{{"type":"post","room":{room},"kind":"claim","body":"x"}}"#);
        assert!(decode::<ClientFrame>(valid.as_bytes()).is_ok());

        for extra in [r#""seq":7"#, r#""from":"shelter""#] {
            let hostile =
                format!(r#"{{"type":"post","room":{room},"kind":"claim","body":"x",{extra}}}"#);
            assert!(
                decode::<ClientFrame>(hostile.as_bytes()).is_err(),
                "accepted {hostile}"
            );
        }
    }

    #[test]
    fn a_client_cannot_send_what_only_the_router_sends() {
        for frame in room_server_frames() {
            let json = serde_json::to_vec(&frame).unwrap();
            assert!(decode::<ClientFrame>(&json).is_err(), "{frame:?}");
        }
    }

    #[test]
    fn a_join_refusal_carries_the_room_and_no_reason() {
        let json = br#"{"type":"join_refused","room":{"project":"example-repo","scope":"project"},"reason":"not_readable"}"#;
        assert!(decode::<ServerFrame>(json).is_err());
    }

    #[test]
    fn a_room_frame_never_renders_its_body_through_debug() {
        let prose = "pasting the thread from #legal";
        let dumps = [
            format!(
                "{:?}",
                ClientFrame::Post {
                    room: room(),
                    kind: MessageKind::Claim,
                    body: body(prose),
                }
            ),
            format!(
                "{:?}",
                ServerFrame::Message {
                    room: room(),
                    seq: RoomSeq(1),
                    from: host(),
                    kind: MessageKind::Claim,
                    body: body(prose),
                }
            ),
        ];
        for dumped in dumps {
            assert!(!dumped.contains("legal"), "{dumped}");
        }
    }

    #[test]
    fn the_largest_room_frames_fit_under_the_frame_cap() {
        // A control character serialises as a six-byte `` escape, which
        // is the most JSON bytes a capped body can become.
        // A `Body` of this build replaces a control character, but
        // a peer built before that can still send one, and the frame cap is
        // checked before any `Body` is built — so the frames are built as
        // JSON values with that older peer's body in place.
        let worst_body = Body::new("x").unwrap();
        let old_peer_body =
            serde_json::Value::String("\u{1}".repeat(sealmesh_chat_wire::MAX_BODY_CHARS));
        let longest_repo = format!("{}/{}", "a".repeat(100), "b".repeat(100));
        let widest_room = RoomId::new(
            ProjectId::try_from("p".repeat(32).as_str()).unwrap(),
            sealmesh_chat_wire::Scope::Repo(RepoSlug::try_from(longest_repo.as_str()).unwrap()),
        );
        let mut message = serde_json::to_value(ServerFrame::Message {
            room: widest_room.clone(),
            seq: RoomSeq(u64::MAX),
            from: HostId::try_from("h".repeat(32).as_str()).unwrap(),
            kind: MessageKind::Handoff,
            body: worst_body.clone(),
        })
        .unwrap();
        *message.get_mut("body").unwrap() = old_peer_body.clone();
        let message = serde_json::to_vec(&message).unwrap();
        let mut post = serde_json::to_value(ClientFrame::Post {
            room: widest_room,
            kind: MessageKind::Handoff,
            body: worst_body,
        })
        .unwrap();
        *post.get_mut("body").unwrap() = old_peer_body;
        let post = serde_json::to_vec(&post).unwrap();

        assert!(
            message.len() > 6 * sealmesh_chat_wire::MAX_BODY_CHARS,
            "the fixture is not the worst case: {} bytes",
            message.len()
        );
        for len in [message.len(), post.len()] {
            assert!(len < MAX_FRAME_BYTES, "{len} bytes");
        }
    }

    /// `ClientFrame` exactly as a router built before rooms knows it.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    enum ClientFrameBeforeRooms {
        Hello { role: Role, token: SecretToken },
        Ask { id: QueryId },
        Answer { id: QueryId, state: HostState },
        Events { batch: EventBatch },
        Projects {},
    }

    #[test]
    fn a_router_built_before_rooms_reads_a_room_frame_as_one_it_cannot_decode() {
        // Undecodable, not misread: that router counts it and keeps the
        // socket, so a new daemon does not read as unreachable.
        for frame in room_client_frames() {
            let json = serde_json::to_vec(&frame).unwrap();
            assert!(
                decode::<ClientFrameBeforeRooms>(&json).is_err(),
                "{frame:?}"
            );
        }
    }

    #[test]
    fn a_reader_built_before_rooms_skips_a_room_frame() {
        for frame in room_server_frames() {
            let json = serde_json::to_vec(&frame).unwrap();
            assert!(
                decode::<ServerFrameBeforePartial>(&json).is_err(),
                "{frame:?}"
            );
        }
    }

    fn entry(seq: u64, text: &str) -> HistoryEntry {
        HistoryEntry {
            seq: RoomSeq(seq),
            from: host(),
            kind: MessageKind::Claim,
            body: body(text),
        }
    }

    fn read_client_frames() -> Vec<ClientFrame> {
        vec![
            ClientFrame::Backfill {
                room: room(),
                after: None,
            },
            ClientFrame::Backfill {
                room: room(),
                after: Some(RoomSeq(12)),
            },
            ClientFrame::ListRooms {},
        ]
    }

    fn read_server_frames() -> Vec<ServerFrame> {
        let mut frames = vec![
            ServerFrame::Backfill {
                room: room(),
                after: Some(RoomSeq(3)),
                messages: vec![entry(4, "taking ENG-557"), entry(6, "done")],
                next: Some(RoomSeq(6)),
            },
            ServerFrame::Backfill {
                room: room(),
                after: None,
                messages: vec![],
                next: None,
            },
            ServerFrame::RoomList {
                rooms: RoomList::new(vec![room()]).unwrap(),
            },
            ServerFrame::RoomList {
                rooms: RoomList::default(),
            },
            ServerFrame::RoomListRefused {},
        ];
        for category in [
            BackfillRefusal::NotHeld,
            BackfillRefusal::Busy,
            BackfillRefusal::Unavailable,
        ] {
            frames.push(ServerFrame::BackfillRefused {
                room: room(),
                category,
            });
        }
        frames
    }

    #[test]
    fn backfill_and_room_list_frames_round_trip() {
        for frame in read_client_frames() {
            let json = serde_json::to_vec(&frame).unwrap();
            assert_eq!(decode::<ClientFrame>(&json).unwrap(), frame);
        }
        for frame in read_server_frames() {
            let json = serde_json::to_vec(&frame).unwrap();
            assert_eq!(decode::<ServerFrame>(&json).unwrap(), frame);
        }
    }

    #[test]
    fn backfill_and_room_list_frames_have_these_wire_words() {
        let room_json = r#"{"project":"example-repo","scope":"project"}"#;
        let project_room = RoomId::new(
            ProjectId::try_from("example-repo").unwrap(),
            sealmesh_chat_wire::Scope::Project,
        );
        let cases: Vec<(String, String)> = vec![
            (
                serde_json::to_string(&ClientFrame::Backfill {
                    room: project_room.clone(),
                    after: Some(RoomSeq(3)),
                })
                .unwrap(),
                format!(r#"{{"type":"backfill","room":{room_json},"after":3}}"#),
            ),
            (
                serde_json::to_string(&ClientFrame::ListRooms {}).unwrap(),
                r#"{"type":"list_rooms"}"#.to_owned(),
            ),
            (
                serde_json::to_string(&ServerFrame::Backfill {
                    room: project_room.clone(),
                    after: None,
                    messages: vec![HistoryEntry {
                        seq: RoomSeq(4),
                        from: host(),
                        kind: MessageKind::Ack,
                        body: body("ok"),
                    }],
                    next: None,
                })
                .unwrap(),
                format!(
                    r#"{{"type":"backfill","room":{room_json},"after":null,"messages":[{{"seq":4,"from":"sandbank","kind":"ack","body":"ok"}}],"next":null}}"#
                ),
            ),
            (
                serde_json::to_string(&ServerFrame::RoomList {
                    rooms: RoomList::new(vec![project_room.clone()]).unwrap(),
                })
                .unwrap(),
                format!(r#"{{"type":"room_list","rooms":[{room_json}]}}"#),
            ),
            (
                serde_json::to_string(&ServerFrame::RoomListRefused {}).unwrap(),
                r#"{"type":"room_list_refused"}"#.to_owned(),
            ),
        ];
        for (category, word) in [
            (BackfillRefusal::NotHeld, "not_held"),
            (BackfillRefusal::Busy, "busy"),
            (BackfillRefusal::Unavailable, "unavailable"),
        ] {
            let frame = ServerFrame::BackfillRefused {
                room: project_room.clone(),
                category,
            };
            assert_eq!(
                serde_json::to_string(&frame).unwrap(),
                format!(r#"{{"type":"backfill_refused","room":{room_json},"category":"{word}"}}"#)
            );
        }
        for (encoded, expected) in cases {
            assert_eq!(encoded, expected);
        }

        // A request with no cursor may leave it out altogether.
        assert_eq!(
            decode::<ClientFrame>(
                format!(r#"{{"type":"backfill","room":{room_json}}}"#).as_bytes()
            )
            .unwrap(),
            ClientFrame::Backfill {
                room: project_room,
                after: None
            }
        );
    }

    #[test]
    fn backfill_and_room_list_frames_refuse_anything_riding_along() {
        let room_json = r#"{"project":"example-repo","scope":"project"}"#;
        let client = [
            format!(r#"{{"type":"backfill","room":{room_json},"after":3,"limit":1000}}"#),
            format!(r#"{{"type":"backfill","room":{room_json},"as":"shelter"}}"#),
            r#"{"type":"list_rooms","project":"example-repo"}"#.to_owned(),
            r#"{"type":"list_rooms","x":1}"#.to_owned(),
        ];
        for hostile in client {
            assert!(
                decode::<ClientFrame>(hostile.as_bytes()).is_err(),
                "accepted {hostile}"
            );
        }
        let server = [
            format!(
                r#"{{"type":"backfill","room":{room_json},"after":null,"messages":[],"next":null,"total":9}}"#
            ),
            format!(
                r#"{{"type":"backfill","room":{room_json},"after":null,"messages":[{{"seq":1,"from":"sandbank","kind":"ack","body":"ok","posted_at":1}}],"next":null}}"#
            ),
            format!(
                r#"{{"type":"backfill_refused","room":{room_json},"category":"not_held","exists":true}}"#
            ),
            format!(
                r#"{{"type":"backfill_refused","room":{room_json},"category":"not_a_member"}}"#
            ),
            format!(r#"{{"type":"backfill_refused","room":{room_json}}}"#),
            format!(r#"{{"type":"room_list","rooms":[{room_json}],"withheld":2}}"#),
            r#"{"type":"room_list_refused","reason":"busy"}"#.to_owned(),
        ];
        for hostile in server {
            assert!(
                decode::<ServerFrame>(hostile.as_bytes()).is_err(),
                "accepted {hostile}"
            );
        }
    }

    #[test]
    fn a_room_list_over_the_cap_can_be_neither_built_nor_decoded() {
        let at_cap = vec![room(); MAX_LISTED_ROOMS];
        assert!(RoomList::new(at_cap).is_ok());
        let over = vec![room(); MAX_LISTED_ROOMS + 1];
        assert_eq!(
            RoomList::new(over.clone()),
            Err(ProtoError::TooManyRooms(MAX_LISTED_ROOMS + 1))
        );
        let json = serde_json::to_vec(&serde_json::json!({
            "type": "room_list",
            "rooms": over,
        }))
        .unwrap();
        assert!(decode::<ServerFrame>(&json).is_err());
    }

    #[test]
    fn a_client_cannot_send_a_backfill_reply_or_a_room_list() {
        for frame in read_server_frames() {
            let json = serde_json::to_vec(&frame).unwrap();
            assert!(decode::<ClientFrame>(&json).is_err(), "{frame:?}");
        }
        for frame in read_client_frames() {
            let json = serde_json::to_vec(&frame).unwrap();
            assert!(decode::<ServerFrame>(&json).is_err(), "{frame:?}");
        }
    }

    #[test]
    fn a_backfill_reply_never_renders_a_body_through_debug() {
        let dumped = format!(
            "{:?}",
            ServerFrame::Backfill {
                room: room(),
                after: None,
                messages: vec![entry(1, "pasting the thread from #legal")],
                next: None,
            }
        );
        assert!(!dumped.contains("legal"), "{dumped}");
    }

    /// `ClientFrame` exactly as a router built after archive-on-merge and
    /// before backfill knows it.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    enum ClientFrameBeforeBackfill {
        Hello {
            role: Role,
            token: SecretToken,
        },
        Ask {
            id: QueryId,
        },
        Answer {
            id: QueryId,
            state: HostState,
        },
        Events {
            batch: EventBatch,
        },
        Projects {},
        Join {
            room: RoomId,
        },
        Leave {
            room: RoomId,
        },
        Post {
            room: RoomId,
            kind: MessageKind,
            body: Body,
        },
    }

    /// `ServerFrame` exactly as a daemon built after archive-on-merge and
    /// before backfill knows it.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    enum ServerFrameBeforeBackfill {
        Welcome {
            host: HostId,
        },
        Query {
            id: QueryId,
            asked_by: HostId,
        },
        Report {
            id: QueryId,
            host: HostId,
            state: HostState,
        },
        Complete {
            id: QueryId,
        },
        Projects {
            projects: Vec<ProjectEntry>,
        },
        Partial {
            id: QueryId,
        },
        Denied {},
        Joined {
            room: RoomId,
        },
        JoinRefused {
            room: RoomId,
        },
        Left {
            room: RoomId,
        },
        Message {
            room: RoomId,
            seq: RoomSeq,
            from: HostId,
            kind: MessageKind,
            body: Body,
        },
        PostRefused {
            room: RoomId,
            category: PostRefusal,
        },
        TurnCapped {
            room: RoomId,
        },
    }

    #[test]
    fn a_router_built_before_backfill_skips_the_requests_and_still_reads_the_next_frame() {
        // An installed router counts a frame it cannot decode and keeps the
        // socket, so a new daemon asking an old router loses only the answer,
        // which it must bound its wait for — and its `Answer`s still land.
        for frame in read_client_frames() {
            let json = serde_json::to_vec(&frame).unwrap();
            assert!(
                decode::<ClientFrameBeforeBackfill>(&json).is_err(),
                "{frame:?}"
            );
        }
        let answer = serde_json::to_vec(&ClientFrame::Answer {
            id: QueryId(3),
            state: HostState::NotSharing {},
        })
        .unwrap();
        assert!(decode::<ClientFrameBeforeBackfill>(&answer).is_ok());
    }

    #[test]
    fn a_daemon_built_before_backfill_skips_the_replies_and_still_reads_the_next_frame() {
        for frame in read_server_frames() {
            let json = serde_json::to_vec(&frame).unwrap();
            assert!(
                decode::<ServerFrameBeforeBackfill>(&json).is_err(),
                "{frame:?}"
            );
        }
        for frame in [
            ServerFrame::Query {
                id: QueryId(9),
                asked_by: host(),
            },
            ServerFrame::Message {
                room: room(),
                seq: RoomSeq(4),
                from: host(),
                kind: MessageKind::Ack,
                body: body("ok"),
            },
            ServerFrame::PostRefused {
                room: room(),
                category: PostRefusal::Archived,
            },
        ] {
            let json = serde_json::to_vec(&frame).unwrap();
            assert!(
                decode::<ServerFrameBeforeBackfill>(&json).is_ok(),
                "{frame:?}"
            );
        }
    }

    fn machine(name: &str) -> MachineName {
        MachineName::try_from(name).unwrap()
    }

    fn machine_client_frames() -> Vec<ClientFrame> {
        vec![
            ClientFrame::Machine {
                name: machine("work-laptop"),
            },
            ClientFrame::AskMachines { id: QueryId(1) },
        ]
    }

    fn machine_server_frames() -> Vec<ServerFrame> {
        vec![
            ServerFrame::MachineReport {
                id: QueryId(1),
                host: host(),
                machine: Some(machine("work-laptop")),
                state: HostState::NotSharing {},
            },
            ServerFrame::MachineReport {
                id: QueryId(1),
                host: host(),
                machine: None,
                state: HostState::Unreachable {},
            },
        ]
    }

    /// Every `type` tag a v0.1.0 peer knows, from that release's own enums.
    /// The machine frames must use none of them: an internally tagged enum
    /// reads an unknown tag as a frame it cannot decode, which every v0.1.0
    /// router, daemon and reader skips while keeping the socket.
    const V0_1_0_CLIENT_TAGS: &[&str] = &[
        "hello",
        "ask",
        "answer",
        "events",
        "projects",
        "join",
        "leave",
        "post",
        "backfill",
        "list_rooms",
    ];
    const V0_1_0_SERVER_TAGS: &[&str] = &[
        "welcome",
        "query",
        "report",
        "complete",
        "projects",
        "partial",
        "denied",
        "joined",
        "join_refused",
        "left",
        "message",
        "post_refused",
        "turn_capped",
        "backfill",
        "backfill_refused",
        "room_list",
        "room_list_refused",
    ];

    fn tag(json: &[u8]) -> String {
        let value: serde_json::Value = serde_json::from_slice(json).unwrap();
        value.get("type").unwrap().as_str().unwrap().to_owned()
    }

    #[test]
    fn the_machine_frames_use_tags_no_v0_1_0_peer_knows() {
        for frame in machine_client_frames() {
            let json = serde_json::to_vec(&frame).unwrap();
            let tag = tag(&json);
            assert!(!V0_1_0_CLIENT_TAGS.contains(&tag.as_str()), "{tag}");
            assert!(
                decode::<ClientFrameBeforeRooms>(&json).is_err(),
                "{frame:?}"
            );
            assert_eq!(decode::<ClientFrame>(&json).unwrap(), frame);
        }
        for frame in machine_server_frames() {
            let json = serde_json::to_vec(&frame).unwrap();
            let tag = tag(&json);
            assert!(!V0_1_0_SERVER_TAGS.contains(&tag.as_str()), "{tag}");
            assert!(
                decode::<ServerFrameBeforePartial>(&json).is_err(),
                "{frame:?}"
            );
            assert_eq!(decode::<ServerFrame>(&json).unwrap(), frame);
        }
    }

    #[test]
    fn the_machine_frames_have_the_shapes_they_are_documented_with() {
        assert_eq!(
            serde_json::to_string(&ClientFrame::Machine {
                name: machine("work-laptop")
            })
            .unwrap(),
            r#"{"type":"machine","name":"work-laptop"}"#
        );
        assert_eq!(
            serde_json::to_string(&ClientFrame::AskMachines { id: QueryId(3) }).unwrap(),
            r#"{"type":"ask_machines","id":3}"#
        );
        assert_eq!(
            serde_json::to_string(&ServerFrame::MachineReport {
                id: QueryId(3),
                host: host(),
                machine: None,
                state: HostState::Unreachable {},
            })
            .unwrap(),
            r#"{"type":"machine_report","id":3,"host":"sandbank","machine":null,"state":{"state":"unreachable"}}"#
        );
    }

    #[test]
    fn a_machine_frame_refuses_anything_riding_along_and_a_bad_name() {
        assert!(decode::<ClientFrame>(br#"{"type":"machine","name":"a","x":1}"#).is_err());
        assert!(decode::<ClientFrame>(br#"{"type":"machine","name":"Work Laptop"}"#).is_err());
        assert!(decode::<ClientFrame>(br#"{"type":"machine","name":""}"#).is_err());
        assert!(decode::<ClientFrame>(br#"{"type":"ask_machines","id":1,"x":1}"#).is_err());
        assert!(
            decode::<ServerFrame>(
                br#"{"type":"machine_report","id":1,"host":"sandbank","machine":null,"state":{"state":"unreachable"},"x":1}"#
            )
            .is_err()
        );
    }

    #[test]
    fn a_machine_name_is_one_dns_label_of_the_host_id_charset() {
        assert!(MachineName::try_from("work-laptop").is_ok());
        assert!(MachineName::try_from("7").is_ok());
        assert!(MachineName::try_from("a".repeat(MAX_MACHINE_NAME).as_str()).is_ok());
        assert!(MachineName::try_from("a".repeat(MAX_MACHINE_NAME + 1).as_str()).is_err());
        assert!(MachineName::try_from("-laptop").is_err());
        assert!(MachineName::try_from("Laptop").is_err());
        assert!(MachineName::try_from("lap.top").is_err());
        assert!(MachineName::try_from("lap\u{202e}top").is_err());
    }

    #[test]
    fn a_hostname_is_folded_into_a_machine_name() {
        let cases = [
            ("Santiagos-MacBook-Pro.local", Some("santiagos-macbook-pro")),
            ("build_box_01", Some("build-box-01")),
            ("--odd  name--", Some("odd-name")),
            ("WORK", Some("work")),
            ("caf\u{e9}-mac", Some("caf-mac")),
            ("", None),
            ("...", None),
            ("___", None),
        ];
        for (hostname, expected) in cases {
            assert_eq!(
                MachineName::from_hostname(hostname)
                    .as_ref()
                    .map(MachineName::as_str),
                expected,
                "{hostname:?}"
            );
        }
        let long = MachineName::from_hostname(&"x".repeat(200)).unwrap();
        assert_eq!(long.as_str().len(), MAX_MACHINE_NAME);
        // A cut that lands on a separator does not leave a trailing `-`.
        let cut = MachineName::from_hostname(&format!("{}-b", "a".repeat(62))).unwrap();
        assert_eq!(cut.as_str(), "a".repeat(62));
    }

    /// `Scope` exactly as v0.1.0 knows it: no `Named`.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "snake_case", deny_unknown_fields)]
    enum ScopeV010 {
        Ticket(sealmesh_wire::TicketId),
        Repo(RepoSlug),
        Project,
    }

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RoomIdV010 {
        project: ProjectId,
        scope: ScopeV010,
    }

    /// The room-carrying frames as a v0.1.0 peer decodes them.
    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    enum RoomFrameV010 {
        Join { room: RoomIdV010 },
        Leave { room: RoomIdV010 },
        Joined { room: RoomIdV010 },
        JoinRefused { room: RoomIdV010 },
        Left { room: RoomIdV010 },
    }

    fn every_room_frame(room: &RoomId) -> Vec<Vec<u8>> {
        let client = [
            ClientFrame::Join { room: room.clone() },
            ClientFrame::Leave { room: room.clone() },
        ]
        .map(|frame| serde_json::to_vec(&frame).unwrap());
        let server = [
            ServerFrame::Joined { room: room.clone() },
            ServerFrame::JoinRefused { room: room.clone() },
            ServerFrame::Left { room: room.clone() },
        ]
        .map(|frame| serde_json::to_vec(&frame).unwrap());
        client.into_iter().chain(server).collect()
    }

    #[test]
    fn a_v010_peer_still_reads_every_scope_it_knows() {
        use sealmesh_chat_wire::Scope;
        for scope in [
            Scope::Ticket(sealmesh_wire::TicketId::new("ENG", 7).unwrap()),
            Scope::Repo(RepoSlug::try_from("example-org/example-repo").unwrap()),
            Scope::Project,
        ] {
            let room = RoomId::new(ProjectId::try_from("example-repo").unwrap(), scope);
            for json in every_room_frame(&room) {
                assert!(
                    decode::<RoomFrameV010>(&json).is_ok(),
                    "{}",
                    String::from_utf8_lossy(&json)
                );
            }
        }
    }

    #[test]
    fn a_named_room_is_undecodable_to_a_v010_peer_like_any_new_variant() {
        // The same treatment `Machine`, `PostRefused` and the rest get: the
        // old decoder fails on the frame, so the peer counts it undecodable
        // and keeps its socket. It must not decode as some other room.
        let named = RoomId::named(sealmesh_chat_wire::RoomName::try_from("ops").unwrap());
        for json in every_room_frame(&named) {
            assert!(
                decode::<RoomFrameV010>(&json).is_err(),
                "{}",
                String::from_utf8_lossy(&json)
            );
            // And the current build reads it back exactly.
            let text = String::from_utf8(json).unwrap();
            assert!(text.contains(r#""scope":{"named":"ops"}"#), "{text}");
            assert!(text.contains(r#""project":"named""#), "{text}");
        }
        let joined = serde_json::to_vec(&ServerFrame::Joined {
            room: named.clone(),
        })
        .unwrap();
        assert_eq!(
            decode::<ServerFrame>(&joined).unwrap(),
            ServerFrame::Joined { room: named }
        );
    }

    #[test]
    fn a_named_room_with_a_bad_name_does_not_decode() {
        for hostile in [
            br#"{"type":"join","room":{"project":"named","scope":{"named":"Ops"}}}"#.as_slice(),
            br#"{"type":"join","room":{"project":"named","scope":{"named":""}}}"#,
            br#"{"type":"join","room":{"project":"named","scope":{"named":"-ops"}}}"#,
            br#"{"type":"join","room":{"project":"named","scope":{"named":"o/ps"}}}"#,
        ] {
            assert!(
                decode::<ClientFrame>(hostile).is_err(),
                "{}",
                String::from_utf8_lossy(hostile)
            );
        }
    }

    #[test]
    fn a_parse_error_carries_only_a_position() {
        let err = decode::<ServerFrame>(br#"{"type":"welcome","host":12345}"#).unwrap_err();
        let rendered = err.to_string();
        assert!(!rendered.contains("12345"), "{rendered}");
    }
}
