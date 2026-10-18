//! The daemon session's side of rooms: commands in, frames out; frames in,
//! inbox entries and replies out.
//!
//! Every method here is synchronous and returns at once. A reply goes back to
//! the waiting local connection over a `oneshot`, whose `send` never blocks,
//! and anything that needs the router is returned as frames for the session
//! to write. Nothing waits for the router's answer: a request is remembered
//! with a deadline, and the answer — or the deadline, checked on the
//! session's tick — resolves it later. That is what lets a posting or
//! backfilling daemon keep answering `Query` at once, so it never reads as
//! `Unreachable` to `who`.
//!
//! The router answers one join, one backfill and one listing per connection
//! at a time, and refuses a second as busy. So each is single-flight here:
//! joins queue (bounded), and a second backfill or listing is answered `busy`
//! locally without bothering the router.
//!
//! Posts are single-flight *per room*, with the rest queued here. A post's
//! answer is either its own delivery fanned back or a `PostRefused`, and the
//! router sends those two by different paths, so they can arrive in either
//! order. With one post in flight per room, both map to that post and to
//! nothing else; with two, a refusal of the second could be pinned on the
//! first.
//!
//! The router's project map is asked for once per connection, right after
//! the re-joins, and kept for that connection. Resolving a checkout's rooms
//! reads it and asks the router nothing, except that a map older than
//! [`MAP_REFRESH`] is asked for again on the resolution that finds it stale,
//! while the old one keeps answering until the new one lands. The router's
//! cut drops repos it could not confirm in time, so a map taken while GitHub
//! was slow would otherwise answer "no rooms" for the life of the connection.
//! A router that never answers — one built before daemons could ask — leaves
//! the map unanswered at the deadline, and every resolution then answers
//! "no rooms" rather than waiting. Either way the map is asked for at most
//! once per [`MAP_RETRY`].
//!
//! Membership lives in memory. After a reconnect every remembered room is
//! re-joined, one at a time; nothing about rooms is written to disk except
//! the audit ledger's refusals and traffic counts, which never hold a body.
//!
//! A room joined through a resolution is held under a lease, renewed each
//! time a resolution names it again and let go once idle for
//! [`LEASE_IDLE`], or at once when the router refuses a post there as
//! archived. A plain `join` is a permanent claim, as it always was. A join
//! the router refuses is remembered for [`REFUSED_JOIN_MEMORY`], and a join
//! of that room meanwhile is refused here without asking again, so a hook
//! resolving on every prompt does not cost the router a GitHub check each
//! time.

use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use jiff::Timestamp;
use sealmesh_chat_wire::{Body, MessageKind, RoomId};
use sealmesh_proto::{ClientFrame, PostRefusal, ProjectEntry, RoomList, RoomSeq, ServerFrame};
use sealmesh_wire::{HostId, RepoSlug, TicketId};
use tokio::sync::oneshot;

use super::{
    Command, Gate, Op, Shared,
    inbox::{Pushed, Unclaim, same_room},
    local::{
        BackfillCategory, Category, ConsumerKey, Delivered, PostCategory, ResolvedRoom, Response,
    },
    resolve,
};
use crate::{
    audit::{self, RoomRecord},
    client::UnknownRefusal,
};

/// How many joins may wait behind the one in flight.
pub const JOIN_QUEUE: usize = 8;

/// How many posts may wait at once, across every room: the one in flight in
/// each room plus those queued behind it.
pub const POSTS_IN_FLIGHT: usize = 8;

/// How many resolutions may wait for the project map at once.
pub const RESOLVE_QUEUE: usize = 8;

/// The least time between two asks for the project map after one went
/// unanswered. An older router never answers, and asking it on every
/// resolution would only add frames it logs as unhandled.
pub const MAP_RETRY: Duration = Duration::from_secs(60);

/// How old a map may get before a resolution asks for a fresh one. A map is
/// the router's cut as it stood when GitHub answered, so one taken during a
/// slow spell — every daemon reconnecting after a deploy, cold caches — can
/// be missing repos; this bounds how long that lasts. Each refresh costs the
/// router one cut on this machine's own token, and the router answers a
/// repeat within its own cooldown from the last cut, which is shorter.
pub const MAP_REFRESH: Duration = Duration::from_secs(10 * 60);

/// How many room and sender pairs one traffic interval records by name.
pub const TRAFFIC_PAIRS: usize = 128;

/// How many refusals one traffic interval records one by one.
pub const REFUSAL_RECORDS: u64 = 32;

/// How long a lease — the claim a resolution's join makes — lasts without
/// being renewed. The hook renews its rooms on every prompt, so this is how
/// long a session may sit idle before the rooms of a checkout it has moved
/// on from stop counting against the room cap. Long enough to outlast a
/// break; a room let go early is only joined again on the next prompt. Like
/// any leave, letting go drops what the room held undrained; it is still in
/// the router's history, for `backfill`.
pub const LEASE_IDLE: Duration = Duration::from_secs(4 * 60 * 60);

/// How long a join the router refused is remembered, and answered here
/// without asking it again. The router's refusal says nothing about why
/// (rule 25), so a refusal that would have passed a moment later — its join
/// rate limit, a slow GitHub — is remembered too. That is the price of not
/// asking GitHub again on every prompt, and it lasts at most this long.
pub const REFUSED_JOIN_MEMORY: Duration = Duration::from_secs(10 * 60);

/// How many refused joins are remembered at once. Past this the oldest is
/// forgotten, which costs at most one more check by the router.
pub const REFUSED_JOINS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    /// How long a join, backfill or listing waits for the router. An older
    /// router never answers any of them.
    pub router_wait: Duration,
    /// How long a post waits for its own delivery or a refusal.
    pub post_wait: Duration,
    /// The least time between two listings the router is asked for. Never
    /// below [`MIN_LIST_COOLDOWN`], whatever this says.
    pub list_cooldown: Duration,
    /// How often received traffic is written to the ledger.
    pub traffic_interval: Duration,
}

/// The floor under [`Timing::list_cooldown`]: a listing spends up to 32
/// GitHub checks from the allowance this host's `who` shares, so asking
/// faster than this, in the spirit of `fleet_status`'s 30s cooldown, would
/// let a looping local client make `who` read partial.
pub const MIN_LIST_COOLDOWN: Duration = Duration::from_secs(30);

impl Default for Timing {
    fn default() -> Self {
        Self {
            router_wait: Duration::from_secs(10),
            post_wait: Duration::from_secs(10),
            list_cooldown: MIN_LIST_COOLDOWN,
            traffic_interval: Duration::from_secs(15 * 60),
        }
    }
}

type Reply = oneshot::Sender<Response>;

fn answer(reply: Reply, response: Response) {
    // The local connection may have given up already; that is its business.
    let _ = reply.send(response);
}

#[derive(Debug)]
struct Join {
    room: RoomId,
    /// `None` for a re-join after a reconnect, which nobody is waiting on —
    /// and which claims nothing new: the room's claims already reflect
    /// whatever held it before the disconnect, so `holder` is ignored
    /// whenever this is `None` (see `joined`).
    reply: Option<Reply>,
    /// The claim this join registers once admitted, ignored when `reply` is
    /// `None`. `None` here (with `reply: Some(_)`) is a real request that
    /// named no holder: an anonymous claim.
    holder: Option<ConsumerKey>,
    /// Whether that claim is a lease (a resolution's join) rather than a
    /// permanent claim (a plain `join`).
    lease: bool,
}

#[derive(Debug)]
struct Pending<T> {
    what: T,
    reply: Reply,
    deadline: Instant,
}

#[derive(Debug)]
struct Post {
    room: RoomId,
    kind: MessageKind,
    body: Body,
    consumer: Option<ConsumerKey>,
    /// Written to the router, and so possibly recorded there. At most one
    /// post per room is.
    sent: bool,
}

#[derive(Debug)]
struct Backfill {
    room: RoomId,
    after: Option<RoomSeq>,
}

#[derive(Debug)]
struct Resolve {
    repo: RepoSlug,
    ticket: Option<TicketId>,
}

/// The router's project map, as far as this connection knows it.
#[derive(Debug, Default)]
struct Map {
    /// What the router last sent, and when: already cut to what this machine
    /// can read.
    known: Option<(Instant, Vec<ProjectEntry>)>,
    /// An ask in flight, waiting until this deadline.
    asking: Option<Instant>,
    /// When it was last asked for on this connection.
    asked: Option<Instant>,
}

impl Map {
    /// Whether a resolution at `now` should ask for the map: none in flight,
    /// none asked within `MAP_RETRY`, and the map missing or stale.
    fn wants_asking(&self, now: Instant) -> bool {
        let stale = self
            .known
            .as_ref()
            .is_none_or(|(at, _)| now.saturating_duration_since(*at) >= MAP_REFRESH);
        let may = self
            .asked
            .is_none_or(|asked| now.saturating_duration_since(asked) >= MAP_RETRY);
        self.asking.is_none() && may && stale
    }

    fn projects(&self) -> &[ProjectEntry] {
        self.known.as_ref().map_or(&[], |(_, projects)| projects)
    }
}

#[derive(Debug)]
struct Traffic {
    since: Timestamp,
    started: Instant,
    counts: Vec<(RoomId, HostId, u64)>,
    elsewhere: u64,
    refusals: u64,
}

impl Traffic {
    fn new(now: Instant) -> Self {
        Self {
            since: Timestamp::now(),
            started: now,
            counts: Vec::new(),
            elsewhere: 0,
            refusals: 0,
        }
    }

    fn count(&mut self, room: &RoomId, from: &HostId) {
        if let Some(entry) = self
            .counts
            .iter_mut()
            .find(|(r, h, _)| h == from && same_room(r, room))
        {
            entry.2 = entry.2.saturating_add(1);
        } else if self.counts.len() < TRAFFIC_PAIRS {
            self.counts.push((room.clone(), from.clone(), 1));
        } else {
            self.elsewhere = self.elsewhere.saturating_add(1);
        }
    }

    fn records(&self, at: Timestamp) -> Vec<RoomRecord> {
        let mut records: Vec<RoomRecord> = self
            .counts
            .iter()
            .map(|(room, from, messages)| RoomRecord::Received {
                at,
                since: self.since,
                room: room.clone(),
                from: from.clone(),
                messages: *messages,
            })
            .collect();
        if self.elsewhere > 0 {
            records.push(RoomRecord::ReceivedElsewhere {
                at,
                since: self.since,
                messages: self.elsewhere,
            });
        }
        let unitemised = self.refusals.saturating_sub(REFUSAL_RECORDS);
        if unitemised > 0 {
            records.push(RoomRecord::RefusalsNotItemised {
                at,
                since: self.since,
                refusals: unitemised,
            });
        }
        records
    }
}

pub struct RoomClient {
    shared: Arc<Shared>,
    gate: Gate,
    timing: Timing,
    ledger: Option<PathBuf>,
    host: Option<HostId>,
    connected: bool,
    join: Option<(Join, Instant)>,
    joins: VecDeque<Join>,
    posts: VecDeque<Pending<Post>>,
    backfill: Option<Pending<Backfill>>,
    list: Option<Pending<()>>,
    listed: Option<(Instant, RoomList)>,
    list_asked: Option<Instant>,
    map: Map,
    resolves: Vec<Pending<Resolve>>,
    project_rooms: bool,
    traffic: Traffic,
    /// Rooms whose join the router refused, and when, oldest first. At most
    /// [`REFUSED_JOINS`].
    refused: VecDeque<(RoomId, Instant)>,
}

impl RoomClient {
    pub fn new(
        shared: Arc<Shared>,
        gate: Gate,
        timing: Timing,
        ledger: Option<PathBuf>,
        now: Instant,
    ) -> Self {
        Self {
            shared,
            gate,
            timing,
            ledger,
            host: None,
            connected: false,
            join: None,
            joins: VecDeque::new(),
            posts: VecDeque::new(),
            backfill: None,
            list: None,
            listed: None,
            list_asked: None,
            map: Map::default(),
            resolves: Vec::new(),
            project_rooms: false,
            traffic: Traffic::new(now),
            refused: VecDeque::new(),
        }
    }

    /// Whether resolving a checkout also names its project's room
    /// (`[chat] project_rooms`). Off unless set.
    #[must_use]
    pub fn with_project_rooms(mut self, on: bool) -> Self {
        self.project_rooms = on;
        self
    }

    fn enabled(&self) -> bool {
        (self.gate)()
    }

    /// A new router connection, as `host`. Re-joins every remembered room,
    /// one at a time.
    pub fn on_connect(&mut self, host: HostId, now: Instant) -> Vec<ClientFrame> {
        self.connected = true;
        self.host = Some(host.clone());
        self.shared.lock().set_connected(Some(host));
        if !self.enabled() {
            return self.stand_down();
        }
        let remembered = self.shared.lock().remembered();
        self.joins.extend(remembered.into_iter().map(|room| Join {
            room,
            reply: None,
            holder: None,
            lease: false,
        }));
        let mut frames = self.pump(now);
        frames.extend(self.ask_for_map(now));
        frames
    }

    /// Asks the router for its project map, and waits for it until the
    /// deadline, checked on the tick.
    fn ask_for_map(&mut self, now: Instant) -> Vec<ClientFrame> {
        self.map.asking = Some(now + self.timing.router_wait);
        self.map.asked = Some(now);
        vec![ClientFrame::Projects {}]
    }

    /// The router connection is gone. Everything waiting on it is answered
    /// `not_connected` — except a post already written, which the router may
    /// have recorded, and which is answered `post_unconfirmed` so an agent
    /// does not retry it into a duplicate. Membership is kept for the next
    /// connection, and so is the traffic interval, so a reconnect does not
    /// reset the cap on itemised refusals.
    pub fn on_disconnect(&mut self) {
        self.connected = false;
        self.map = Map::default();
        self.shared.lock().set_connected(None);
        self.settle_all(|| Response::NotConnected {});
    }

    /// Chat was turned off: withdraw from every held room with a `Leave`,
    /// forget every room, and answer everything waiting. After this nothing
    /// more about rooms is sent, and whatever the router delivers is dropped
    /// on arrival.
    fn stand_down(&mut self) -> Vec<ClientFrame> {
        self.settle_all(|| Response::Disabled {});
        self.refused.clear();
        let held = {
            let mut state = self.shared.lock();
            let held = state.held_rooms();
            state.clear();
            held
        };
        self.shared.notify();
        if !self.connected {
            return Vec::new();
        }
        held.into_iter()
            .map(|room| ClientFrame::Leave { room })
            .collect()
    }

    fn settle_all(&mut self, response: impl Fn() -> Response) {
        if let Some((join, _)) = self.join.take()
            && let Some(reply) = join.reply
        {
            answer(reply, response());
        }
        for join in self.joins.drain(..) {
            if let Some(reply) = join.reply {
                answer(reply, response());
            }
        }
        for post in self.posts.drain(..) {
            if post.what.sent {
                let room = post.what.room;
                answer(post.reply, Response::PostUnconfirmed { room });
            } else {
                answer(post.reply, response());
            }
        }
        if let Some(backfill) = self.backfill.take() {
            answer(backfill.reply, response());
        }
        if let Some(list) = self.list.take() {
            answer(list.reply, response());
        }
        for resolve in self.resolves.drain(..) {
            answer(resolve.reply, response());
        }
    }

    /// Sends the next queued join if none is in flight.
    ///
    /// A queued join whose caller has stopped waiting is dropped rather than
    /// sent: the caller was already told `timed_out`, and admitting it now
    /// would leave this daemon holding a room — and counting it against the
    /// room cap — that its client was told it did not get. A re-join after a
    /// reconnect has no caller and is always sent.
    fn pump(&mut self, now: Instant) -> Vec<ClientFrame> {
        if self.join.is_some() || !self.connected {
            return Vec::new();
        }
        let join = loop {
            let Some(mut join) = self.joins.pop_front() else {
                return Vec::new();
            };
            let Some(reply) = join.reply.take() else {
                break join;
            };
            if reply.is_closed() {
                continue;
            }
            // Refused while this one waited in the queue.
            if self.refused_recently(&join.room, now) {
                answer(reply, Response::JoinRefused { room: join.room });
                continue;
            }
            join.reply = Some(reply);
            break join;
        };
        let frame = ClientFrame::Join {
            room: join.room.clone(),
        };
        self.join = Some((join, now + self.timing.router_wait));
        vec![frame]
    }

    pub fn on_command(&mut self, command: Command, now: Instant) -> Vec<ClientFrame> {
        let Command { op, reply } = command;
        if !self.enabled() {
            let frames = self.stand_down();
            answer(reply, Response::Disabled {});
            return frames;
        }
        if !self.connected {
            answer(reply, Response::NotConnected {});
            return Vec::new();
        }
        match op {
            Op::Join(room, holder) => self.join(room, holder, false, reply, now),
            Op::Hold(room, holder) => self.join(room, holder, true, reply, now),
            Op::Leave(room, holder) => self.leave(&room, holder, reply),
            Op::Post {
                room,
                kind,
                body,
                consumer,
            } => self.post(room, kind, body, consumer, reply, now),
            Op::Backfill { room, after } => self.backfill(room, after, reply, now),
            Op::List => self.list(reply, now),
            Op::Resolve { repo, ticket } => self.resolve(repo, ticket, reply, now),
        }
    }

    /// Answers from the map this connection holds, or, when it holds none
    /// yet, waits for the one being asked for. A stale map is refreshed
    /// while it keeps answering. Never waits on anything else, and never past
    /// the map's own deadline.
    fn resolve(
        &mut self,
        repo: RepoSlug,
        ticket: Option<TicketId>,
        reply: Reply,
        now: Instant,
    ) -> Vec<ClientFrame> {
        let what = Resolve { repo, ticket };
        let frames = if self.map.wants_asking(now) {
            self.ask_for_map(now)
        } else {
            Vec::new()
        };
        let Some(deadline) = self.map.asking.filter(|_| self.map.known.is_none()) else {
            answer(reply, self.resolved(&what));
            return frames;
        };
        if self.resolves.len() >= RESOLVE_QUEUE {
            answer(reply, Response::Busy {});
            return frames;
        }
        self.resolves.push(Pending {
            what,
            reply,
            deadline,
        });
        frames
    }

    /// The answer to one resolution from the map as it stands. Anything but a
    /// map the router sent reads as an empty one.
    fn resolved(&self, what: &Resolve) -> Response {
        let rooms = resolve::rooms_for(
            self.map.projects(),
            &what.repo,
            what.ticket.as_ref(),
            self.project_rooms,
        );
        Response::Resolved {
            rooms: rooms
                .into_iter()
                .map(|room| ResolvedRoom { room, join: None })
                .collect(),
        }
    }

    /// Answers every resolution waiting on the map, from the map as it now
    /// stands.
    fn answer_resolves(&mut self) {
        for pending in std::mem::take(&mut self.resolves) {
            let response = self.resolved(&pending.what);
            answer(pending.reply, response);
        }
    }

    /// `holder` is the claim this join registers: `None` for an anonymous,
    /// permanent claim (an MCP tool's `join`, or any caller that does not
    /// distinguish itself), `Some` for one this daemon tracks by name. A room
    /// already held admits the new claim at once, with no new round trip to
    /// the router and without counting against the join rate limit — the
    /// same idempotence a repeated anonymous join always had, extended to a
    /// second distinct holder joining a room someone else already holds. A
    /// *new* named claim past `inbox::MAX_NAMED_CLAIMS` on one room is
    /// answered `busy`, not admitted and not evicted — the room itself is
    /// unaffected, since whatever already claims it still does.
    ///
    /// With `lease`, the claim is `holder`'s lease rather than a permanent
    /// claim, and joining a room already held renews it. Leases idle past
    /// [`LEASE_IDLE`] are let go first, so the room cap never counts a room
    /// only a stale lease held. A room the router refused within
    /// [`REFUSED_JOIN_MEMORY`] is refused here without asking it again.
    fn join(
        &mut self,
        room: RoomId,
        holder: Option<ConsumerKey>,
        lease: bool,
        reply: Reply,
        now: Instant,
    ) -> Vec<ClientFrame> {
        let mut frames = self.expire_leases(now);
        let (held, claimed, remembered, count, cap) = {
            let mut state = self.shared.lock();
            let held = state.held(&room);
            let claimed = held.is_none()
                || if lease {
                    state.lease(&room, holder.as_ref(), now)
                } else {
                    state.claim(&room, holder.as_ref())
                };
            (
                held,
                claimed,
                state.remembers(&room),
                state.room_count(),
                state.limits().rooms,
            )
        };
        if let Some(held) = held {
            if claimed {
                answer(reply, Response::Joined { room: held });
            } else {
                answer(reply, Response::Busy {});
            }
            return frames;
        }
        if self.refused_recently(&room, now) {
            answer(reply, Response::JoinRefused { room });
            return frames;
        }
        if !remembered && count >= cap {
            answer(reply, Response::RoomLimit {});
            return frames;
        }
        if self.joins.len() >= JOIN_QUEUE {
            answer(reply, Response::Busy {});
            return frames;
        }
        self.joins.push_back(Join {
            room,
            reply: Some(reply),
            holder,
            lease,
        });
        frames.extend(self.pump(now));
        frames
    }

    /// Whether the router refused a join of `room` within
    /// [`REFUSED_JOIN_MEMORY`] of `now`. Forgets what has aged out.
    fn refused_recently(&mut self, room: &RoomId, now: Instant) -> bool {
        self.refused
            .retain(|(_, at)| now.saturating_duration_since(*at) < REFUSED_JOIN_MEMORY);
        self.refused.iter().any(|(r, _)| same_room(r, room))
    }

    /// Remembers that the router refused a join of `room` at `now`.
    fn remember_refusal(&mut self, room: &RoomId, now: Instant) {
        self.refused.retain(|(r, _)| !same_room(r, room));
        if self.refused.len() >= REFUSED_JOINS {
            self.refused.pop_front();
        }
        self.refused.push_back((room.clone(), now));
    }

    /// Lets go of every lease idle for [`LEASE_IDLE`], and of each room that
    /// leaves with no claim, telling the router about those it held.
    fn expire_leases(&mut self, now: Instant) -> Vec<ClientFrame> {
        let released = self.shared.lock().expire_leases(now, LEASE_IDLE);
        self.release(released)
    }

    /// The frames and queue changes for rooms just forgotten because their
    /// last claim went: a `Leave` for each one held, and no queued re-join
    /// left for any room no longer remembered, which would bring it back
    /// with no claim on it.
    fn release(&mut self, released: Vec<Unclaim>) -> Vec<ClientFrame> {
        if released.is_empty() {
            return Vec::new();
        }
        self.shared.notify();
        let frames = released
            .into_iter()
            .filter_map(|unclaim| match unclaim {
                Unclaim::Forgotten(room) => Some(ClientFrame::Leave { room }),
                Unclaim::ForgottenUnheld | Unclaim::NotMember | Unclaim::StillClaimed => None,
            })
            .collect();
        let state = self.shared.lock();
        self.joins
            .retain(|j| j.reply.is_some() || state.remembers(&j.room));
        drop(state);
        frames
    }

    /// `holder: None` clears the anonymous/permanent slot, `holder:
    /// Some(key)` clears that one named claim; the room is actually left,
    /// its undrained messages dropped and the router told, only once no
    /// claim remains (see `inbox::State::unclaim`).
    fn leave(
        &mut self,
        room: &RoomId,
        holder: Option<ConsumerKey>,
        reply: Reply,
    ) -> Vec<ClientFrame> {
        let mut queued = VecDeque::new();
        for join in self.joins.drain(..) {
            // A queued join registering the exact same claim this leave
            // targets is refused outright: admitting it after would put the
            // claim this leave just removed right back.
            if same_room(&join.room, room) && join.holder == holder {
                if let Some(waiting) = join.reply {
                    answer(waiting, Response::JoinRefused { room: join.room });
                }
            } else {
                queued.push_back(join);
            }
        }
        self.joins = queued;

        let unclaim = self.shared.lock().unclaim(room, holder.as_ref());
        self.shared.notify();

        // If the room is now forgotten outright, a reconnect's own queued
        // re-join for it (`reply: None`, claiming nothing new — see `join`
        // and `joined`) must not bring it back once it is eventually sent
        // and admitted with no claim on it. This is not covered by the loop
        // above: a *named* leave that empties the room does not match a
        // re-join's `holder: None`.
        if matches!(unclaim, Unclaim::Forgotten(_) | Unclaim::ForgottenUnheld) {
            self.joins
                .retain(|j| !(same_room(&j.room, room) && j.reply.is_none()));
        }

        match unclaim {
            Unclaim::NotMember => {
                answer(reply, Response::NotMember { room: room.clone() });
                Vec::new()
            }
            Unclaim::StillClaimed | Unclaim::ForgottenUnheld => {
                answer(reply, Response::Left { room: room.clone() });
                Vec::new()
            }
            Unclaim::Forgotten(held) => {
                answer(reply, Response::Left { room: held.clone() });
                vec![ClientFrame::Leave { room: held }]
            }
        }
    }

    fn post(
        &mut self,
        room: RoomId,
        kind: MessageKind,
        body: Body,
        consumer: Option<ConsumerKey>,
        reply: Reply,
        now: Instant,
    ) -> Vec<ClientFrame> {
        let Some(held) = self.shared.lock().held(&room) else {
            answer(reply, Response::NotMember { room });
            return Vec::new();
        };
        if self.posts.len() >= POSTS_IN_FLIGHT {
            answer(reply, Response::Busy {});
            return Vec::new();
        }
        self.posts.push_back(Pending {
            what: Post {
                room: held.clone(),
                kind,
                body,
                consumer,
                sent: false,
            },
            reply,
            deadline: now + self.timing.post_wait,
        });
        self.send_next_post(&held)
    }

    /// Writes the oldest queued post for `room`, unless one is in flight
    /// there already.
    fn send_next_post(&mut self, room: &RoomId) -> Vec<ClientFrame> {
        let in_room = |p: &Pending<Post>| same_room(&p.what.room, room);
        if self.posts.iter().any(|p| in_room(p) && p.what.sent) {
            return Vec::new();
        }
        let Some(next) = self.posts.iter_mut().find(|p| in_room(p)) else {
            return Vec::new();
        };
        next.what.sent = true;
        vec![ClientFrame::Post {
            room: next.what.room.clone(),
            kind: next.what.kind,
            body: next.what.body.clone(),
        }]
    }

    /// Takes the post in flight in `room`, if there is one.
    fn take_in_flight(&mut self, room: &RoomId) -> Option<Pending<Post>> {
        let index = self
            .posts
            .iter()
            .position(|p| p.what.sent && same_room(&p.what.room, room))?;
        self.posts.remove(index)
    }

    fn backfill(
        &mut self,
        room: RoomId,
        after: Option<RoomSeq>,
        reply: Reply,
        now: Instant,
    ) -> Vec<ClientFrame> {
        let Some(held) = self.shared.lock().held(&room) else {
            answer(reply, Response::NotMember { room });
            return Vec::new();
        };
        if self.backfill.is_some() {
            answer(reply, Response::Busy {});
            return Vec::new();
        }
        self.backfill = Some(Pending {
            what: Backfill {
                room: held.clone(),
                after,
            },
            reply,
            deadline: now + self.timing.router_wait,
        });
        vec![ClientFrame::Backfill { room: held, after }]
    }

    fn list(&mut self, reply: Reply, now: Instant) -> Vec<ClientFrame> {
        if self.list.is_some() {
            answer(reply, Response::Busy {});
            return Vec::new();
        }
        let cooldown = self.timing.list_cooldown.max(MIN_LIST_COOLDOWN);
        if let Some(asked) = self.list_asked
            && now.saturating_duration_since(asked) < cooldown
        {
            let response = match &self.listed {
                Some((at, rooms)) => Response::Rooms {
                    rooms: rooms.clone(),
                    age_secs: now.saturating_duration_since(*at).as_secs(),
                },
                None => Response::ListThrottled {
                    retry_after_secs: cooldown
                        .saturating_sub(now.saturating_duration_since(asked))
                        .as_secs()
                        .max(1),
                },
            };
            answer(reply, response);
            return Vec::new();
        }
        self.list_asked = Some(now);
        self.list = Some(Pending {
            what: (),
            reply,
            deadline: now + self.timing.router_wait,
        });
        vec![ClientFrame::ListRooms {}]
    }

    /// One room frame from the router. Anything else is ignored.
    pub fn on_frame(&mut self, frame: ServerFrame, now: Instant) -> Vec<ClientFrame> {
        if !self.enabled() {
            // The frame is dropped, whatever it is. The first one after chat
            // goes off also withdraws from what is held.
            return self.stand_down();
        }
        match frame {
            ServerFrame::Joined { room } => self.joined(room, now),
            ServerFrame::JoinRefused { room } => self.join_refused(&room, now),
            ServerFrame::Left { room } => {
                if self.shared.lock().forget(&room) {
                    self.shared.notify();
                }
                Vec::new()
            }
            ServerFrame::Message {
                room,
                seq,
                from,
                kind,
                body,
            } => self.message(room, seq, from, kind, body),
            ServerFrame::PostRefused { room, category } => {
                self.post_refused(&room, Category::Known(category))
            }
            ServerFrame::TurnCapped { room } => {
                self.shared.lock().note_turn_capped(&room, true);
                Vec::new()
            }
            ServerFrame::Backfill {
                room,
                after,
                messages,
                next,
            } => {
                let matches = self.backfill.as_ref().is_some_and(|pending| {
                    same_room(&pending.what.room, &room) && pending.what.after == after
                });
                // A reply whose cursor is not the one waiting is the late
                // answer to a request that already timed out: dropped.
                if matches && let Some(pending) = self.backfill.take() {
                    answer(
                        pending.reply,
                        Response::Backfill {
                            room,
                            after,
                            messages,
                            next,
                        },
                    );
                }
                Vec::new()
            }
            ServerFrame::BackfillRefused { room, category } => {
                self.backfill_refused(&room, Category::Known(category));
                Vec::new()
            }
            ServerFrame::RoomList { rooms } => {
                if let Some(pending) = self.list.take() {
                    self.listed = Some((now, rooms.clone()));
                    answer(pending.reply, Response::Rooms { rooms, age_secs: 0 });
                }
                Vec::new()
            }
            ServerFrame::RoomListRefused {} => {
                if let Some(pending) = self.list.take() {
                    answer(pending.reply, Response::RoomListRefused {});
                }
                Vec::new()
            }
            // The router's map, cut to what this machine can read. Taken
            // whenever it comes on this connection, even after the deadline:
            // it is the same router's answer, only late.
            ServerFrame::Projects { projects } => {
                self.map.known = Some((now, projects));
                self.map.asking = None;
                self.answer_resolves();
                Vec::new()
            }
            ServerFrame::Welcome { .. }
            | ServerFrame::Query { .. }
            | ServerFrame::Report { .. }
            | ServerFrame::Complete { .. }
            | ServerFrame::Partial { .. }
            | ServerFrame::Denied {} => Vec::new(),
        }
    }

    /// A refusal whose category this build does not know (`client::classify`).
    pub fn on_unknown_refusal(&mut self, refusal: UnknownRefusal) -> Vec<ClientFrame> {
        if !self.enabled() {
            return self.stand_down();
        }
        match refusal {
            UnknownRefusal::Post { room } => self.post_refused(&room, Category::UNKNOWN),
            UnknownRefusal::Backfill { room } => {
                self.backfill_refused(&room, Category::UNKNOWN);
                Vec::new()
            }
        }
    }

    fn joined(&mut self, room: RoomId, now: Instant) -> Vec<ClientFrame> {
        let answers_ours = self
            .join
            .as_ref()
            .is_some_and(|(join, _)| same_room(&join.room, &room));
        if !answers_ours {
            // A re-affirmation of a room already remembered, or a late answer
            // to a join that timed out. Only a room this daemon remembers is
            // taken up; the router cannot make it hold one it never asked for.
            // Anything else is left at once, so the router does not keep
            // delivering to, counting, and turn-capping a member that is not
            // there.
            let mut state = self.shared.lock();
            if state.remembers(&room) {
                state.admit(room);
                return Vec::new();
            }
            return vec![ClientFrame::Leave { room }];
        }
        let mut frames = Vec::new();
        let Some((join, _)) = self.join.take() else {
            return frames;
        };
        // A reconnect's re-join (`reply: None`) claims nothing new: the
        // room's claims already reflect whatever held it before the
        // disconnect. But a concurrent `Leave` may have forgotten this very
        // room while the re-join was already in flight to the router — in
        // that case `admit` would resurrect it with no claim on it, held
        // forever by nobody. Check first, and release it right back instead.
        if join.reply.is_none() {
            if self.shared.lock().remembers(&room) {
                self.shared.lock().admit(room.clone());
            } else {
                frames.push(ClientFrame::Leave { room });
            }
            frames.extend(self.pump(now));
            return frames;
        }
        // A real request. Its `reply` may already be closed — the caller
        // gave up (a signal, its own timeout) before this answer arrived —
        // in which case its claim must not be registered: nobody is left to
        // ever release it. `answer` below is a safe no-op on a closed
        // channel regardless, but the *claim* is the part that must not
        // outlive the caller.
        let reply_live = join.reply.as_ref().is_some_and(|r| !r.is_closed());
        let admitted = self.shared.lock().admit(room.clone());
        if !admitted {
            // Filled up while the join was in flight: leave again so the
            // router and this daemon agree.
            frames.push(ClientFrame::Leave { room: room.clone() });
            if let Some(reply) = join.reply {
                answer(reply, Response::RoomLimit {});
            }
            frames.extend(self.pump(now));
            return frames;
        }
        self.refused.retain(|(r, _)| !same_room(r, &room));
        if reply_live {
            let claimed = {
                let mut state = self.shared.lock();
                if join.lease {
                    state.lease(&room, join.holder.as_ref(), now)
                } else {
                    state.claim(&room, join.holder.as_ref())
                }
            };
            let response = if claimed {
                Response::Joined { room: room.clone() }
            } else {
                // Someone else's 64 named claims already fill this room;
                // this caller's own claim is refused, not evicted, and the
                // room is unaffected for whoever already holds it.
                Response::Busy {}
            };
            if let Some(reply) = join.reply {
                answer(reply, response);
            }
        } else if self.shared.lock().claims_are_empty(&room) {
            // The caller is gone and nothing else claims this room: an
            // admitted room with no claim on it would sit held forever,
            // released by nobody. Release it now, the same treatment `pump`
            // gives a queued join whose caller already gave up.
            if let Unclaim::Forgotten(held) = self.shared.lock().unclaim(&room, None) {
                frames.push(ClientFrame::Leave { room: held });
            }
        }
        frames.extend(self.pump(now));
        frames
    }

    fn join_refused(&mut self, room: &RoomId, now: Instant) -> Vec<ClientFrame> {
        let answers_ours = self
            .join
            .as_ref()
            .is_some_and(|(join, _)| same_room(&join.room, room));
        if !answers_ours {
            return Vec::new();
        }
        if let Some((join, _)) = self.join.take() {
            // Only a join a caller asked for: a reconnect's re-join is refused
            // most often while the router is busy re-checking every daemon
            // after a restart, and remembering that would keep the next
            // caller out of a room it may well be let into.
            if join.reply.is_some() {
                self.remember_refusal(&join.room, now);
            }
            // A remembered room the router no longer admits is forgotten.
            if self.shared.lock().forget(&join.room) {
                self.shared.notify();
            }
            if let Some(reply) = join.reply {
                answer(reply, Response::JoinRefused { room: join.room });
            }
        }
        self.pump(now)
    }

    fn message(
        &mut self,
        room: RoomId,
        seq: RoomSeq,
        from: HostId,
        kind: MessageKind,
        body: Body,
    ) -> Vec<ClientFrame> {
        // This daemon's own post, fanned back to it: that is the post's
        // confirmation, and its seq. Only the post in flight in this room can
        // be the one, and it must match on kind and body as well, since two
        // laptops logged in as one person share a host.
        let mut origin = None;
        let mut frames = Vec::new();
        let ours = self.host.as_ref() == Some(&from)
            && self.posts.iter().any(|p| {
                p.what.sent
                    && same_room(&p.what.room, &room)
                    && p.what.kind == kind
                    && p.what.body == body
            });
        if ours && let Some(pending) = self.take_in_flight(&room) {
            origin = pending.what.consumer;
            answer(
                pending.reply,
                Response::Posted {
                    room: room.clone(),
                    seq,
                },
            );
            self.shared.lock().note_turn_capped(&room, false);
            frames = self.send_next_post(&room);
        }
        let delivered = Delivered {
            room,
            seq,
            from,
            kind,
            body,
            received_at: Timestamp::now(),
        };
        let (room, from) = (delivered.room.clone(), delivered.from.clone());
        let pushed = self.shared.lock().push(delivered, origin);
        if pushed == Pushed::Stored {
            self.traffic.count(&room, &from);
            self.shared.notify();
        }
        frames
    }

    /// A refusal answers the post in flight in its room, and nothing else. One
    /// that answers no post of ours is dropped — not answered, not counted,
    /// not written to the ledger — so a router cannot write to the ledger at
    /// will.
    ///
    /// A refusal as `archived` also lets go of every lease on the room: its
    /// ticket has merged, so a checkout following it has nothing more to
    /// hear there. A room no other claim holds is left, and the posts queued
    /// behind this one are refused the same way, since archiving is final.
    fn post_refused(&mut self, room: &RoomId, category: PostCategory) -> Vec<ClientFrame> {
        let Some(pending) = self.take_in_flight(room) else {
            return Vec::new();
        };
        let room = pending.what.room;
        if category == Category::Known(PostRefusal::Archived) {
            answer(
                pending.reply,
                Response::PostRefused {
                    room: room.clone(),
                    category,
                },
            );
            return self.archived(&room);
        }
        self.shared.lock().note_refusal(&room, category);
        answer(
            pending.reply,
            Response::PostRefused {
                room: room.clone(),
                category,
            },
        );
        self.record_refusal(RoomRecord::PostRefused {
            at: Timestamp::now(),
            room: room.clone(),
            category,
        });
        self.send_next_post(&room)
    }

    /// The router refused a post of ours in `room` as archived, already
    /// answered. Recorded like any refusal; then every lease on the room is
    /// let go, and if nothing else holds it, it is left and what was queued
    /// for it is refused as archived too.
    fn archived(&mut self, room: &RoomId) -> Vec<ClientFrame> {
        let category = Category::Known(PostRefusal::Archived);
        self.shared.lock().note_refusal(room, category);
        self.record_refusal(RoomRecord::PostRefused {
            at: Timestamp::now(),
            room: room.clone(),
            category,
        });
        let unclaim = self.shared.lock().drop_leases(room);
        if !matches!(unclaim, Unclaim::Forgotten(_) | Unclaim::ForgottenUnheld) {
            return self.send_next_post(room);
        }
        let (queued, kept): (VecDeque<_>, VecDeque<_>) = std::mem::take(&mut self.posts)
            .into_iter()
            .partition(|p| same_room(&p.what.room, room));
        self.posts = kept;
        for post in queued {
            answer(
                post.reply,
                Response::PostRefused {
                    room: post.what.room,
                    category,
                },
            );
        }
        self.release(vec![unclaim])
    }

    /// Like a post refusal, one that answers no backfill of ours is dropped.
    fn backfill_refused(&mut self, room: &RoomId, category: BackfillCategory) {
        let matches = self
            .backfill
            .as_ref()
            .is_some_and(|pending| same_room(&pending.what.room, room));
        if !matches {
            return;
        }
        let Some(pending) = self.backfill.take() else {
            return;
        };
        if category.is_unknown() {
            self.shared.lock().note_unknown_refusal();
            self.record_refusal(RoomRecord::BackfillRefused {
                at: Timestamp::now(),
                room: pending.what.room.clone(),
                category,
            });
        }
        answer(
            pending.reply,
            Response::BackfillRefused {
                room: pending.what.room,
                category,
            },
        );
    }

    /// Writes a refusal to the ledger, at most `REFUSAL_RECORDS` an interval;
    /// the rest are counted and written as one line when the interval ends,
    /// so a router that floods refusals cannot flood the disk.
    fn record_refusal(&mut self, record: RoomRecord) {
        self.traffic.refusals = self.traffic.refusals.saturating_add(1);
        if self.traffic.refusals <= REFUSAL_RECORDS {
            self.write(&record);
        }
    }

    fn write(&self, record: &RoomRecord) {
        let Some(path) = &self.ledger else { return };
        if let Err(error) = audit::record_room(path, record) {
            eprintln!("could not write the audit log: {error:#}");
        }
    }

    /// Writes the interval's traffic once the interval has ended. Only that
    /// ends it: a reconnect does not, so the cap on itemised refusals spans
    /// reconnects.
    fn flush_traffic(&mut self, now: Instant) {
        if now.saturating_duration_since(self.traffic.started) < self.timing.traffic_interval {
            return;
        }
        let done = std::mem::replace(&mut self.traffic, Traffic::new(now));
        for record in done.records(Timestamp::now()) {
            self.write(&record);
        }
    }

    /// The session's periodic tick: chat turned off, deadlines passed, the
    /// traffic interval ended.
    pub fn on_tick(&mut self, now: Instant) -> Vec<ClientFrame> {
        if !self.enabled() {
            let frames = self.stand_down();
            self.flush_traffic(now);
            return frames;
        }

        if self
            .join
            .as_ref()
            .is_some_and(|(_, deadline)| now >= *deadline)
            && let Some((join, _)) = self.join.take()
            && let Some(reply) = join.reply
        {
            answer(reply, Response::TimedOut {});
        }
        let mut waiting = VecDeque::new();
        let mut freed = Vec::new();
        for post in self.posts.drain(..) {
            if now < post.deadline {
                waiting.push_back(post);
                continue;
            }
            let room = post.what.room;
            if post.what.sent {
                // Written, and neither delivered back nor refused: it may
                // have landed.
                answer(post.reply, Response::PostUnconfirmed { room: room.clone() });
                freed.push(room);
            } else {
                // Never written, so safe to send again.
                answer(post.reply, Response::TimedOut {});
            }
        }
        self.posts = waiting;
        let mut frames = Vec::new();
        for room in freed {
            frames.extend(self.send_next_post(&room));
        }
        if self.backfill.as_ref().is_some_and(|p| now >= p.deadline)
            && let Some(pending) = self.backfill.take()
        {
            answer(pending.reply, Response::TimedOut {});
        }
        if self.list.as_ref().is_some_and(|p| now >= p.deadline)
            && let Some(pending) = self.list.take()
        {
            answer(pending.reply, Response::TimedOut {});
        }
        // An unanswered map is an empty one: every resolution waiting on it
        // is answered "no rooms", the same as a repo in no project.
        if self.map.asking.is_some_and(|deadline| now >= deadline) {
            self.map.asking = None;
            self.answer_resolves();
        }

        frames.extend(self.expire_leases(now));
        self.flush_traffic(now);
        frames.extend(self.pump(now));
        frames
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

    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::room::local::ResolvedRoom;
    use sealmesh_chat_wire::Scope;
    use sealmesh_proto::{BackfillRefusal, HistoryEntry, PostRefusal};
    use sealmesh_wire::{ProjectId, RepoSlug, TicketId};

    use super::*;
    use crate::room::inbox::{self, Limits, State};

    struct Rig {
        client: RoomClient,
        shared: Arc<Shared>,
        on: Arc<AtomicBool>,
        t0: Instant,
    }

    fn rig_with(ledger: Option<PathBuf>) -> Rig {
        let shared = Shared::new(State::new(Limits::default(), Timestamp::now()));
        let on = Arc::new(AtomicBool::new(true));
        let gate_on = on.clone();
        let t0 = Instant::now();
        let client = RoomClient::new(
            shared.clone(),
            Arc::new(move || gate_on.load(Ordering::SeqCst)),
            Timing::default(),
            ledger,
            t0,
        );
        Rig {
            client,
            shared,
            on,
            t0,
        }
    }

    fn rig() -> Rig {
        rig_with(None)
    }

    fn host(name: &str) -> HostId {
        HostId::try_from(name).unwrap()
    }

    fn repo(slug: &str) -> RoomId {
        RoomId::new(
            ProjectId::try_from("example-repo").unwrap(),
            Scope::Repo(RepoSlug::try_from(slug).unwrap()),
        )
    }

    fn ticket(n: u32) -> RoomId {
        RoomId::new(
            ProjectId::try_from("example-repo").unwrap(),
            Scope::Ticket(TicketId::new("ENG", n).unwrap()),
        )
    }

    fn command(op: Op) -> (Command, oneshot::Receiver<Response>) {
        let (reply, rx) = oneshot::channel();
        (Command { op, reply }, rx)
    }

    fn body(text: &str) -> Body {
        Body::new(text).unwrap()
    }

    fn now(rx: &mut oneshot::Receiver<Response>) -> Option<Response> {
        rx.try_recv().ok()
    }

    impl Rig {
        fn at(&self, secs: u64) -> Instant {
            self.t0 + Duration::from_secs(secs)
        }

        fn connect(&mut self) -> Vec<ClientFrame> {
            self.client.on_connect(host("sandbank"), self.t0)
        }

        fn join(&mut self, room: RoomId) -> Response {
            self.join_as(room, None)
        }

        /// Joins `room` with `holder`'s claim, driving a full request/answer
        /// round trip through the router the way `join` does.
        fn join_as(&mut self, room: RoomId, holder: Option<ConsumerKey>) -> Response {
            let (cmd, mut rx) = command(Op::Join(room.clone(), holder));
            let frames = self.client.on_command(cmd, self.t0);
            assert_eq!(frames, vec![ClientFrame::Join { room: room.clone() }]);
            assert!(now(&mut rx).is_none(), "answered before the router did");
            self.client.on_frame(ServerFrame::Joined { room }, self.t0);
            now(&mut rx).unwrap()
        }

        /// Joins `room` with `holder`'s claim when the room is already held —
        /// answered at once, with no new frame to the router.
        fn join_as_fast(&mut self, room: RoomId, holder: Option<ConsumerKey>) -> Response {
            let (cmd, mut rx) = command(Op::Join(room.clone(), holder));
            let frames = self.client.on_command(cmd, self.t0);
            assert!(frames.is_empty(), "a room already held sent a new frame");
            now(&mut rx).unwrap()
        }

        fn leave_as(
            &mut self,
            room: RoomId,
            holder: Option<ConsumerKey>,
        ) -> (Vec<ClientFrame>, Response) {
            let (cmd, mut rx) = command(Op::Leave(room, holder));
            let frames = self.client.on_command(cmd, self.t0);
            (frames, now(&mut rx).unwrap())
        }
    }

    #[test]
    fn a_join_is_answered_when_the_router_answers_and_not_before() {
        let mut rig = rig();
        rig.connect();
        let room = repo("example-org/example-repo");
        assert_eq!(
            rig.join(room.clone()),
            Response::Joined { room: room.clone() }
        );
        assert!(rig.shared.lock().held(&room).is_some());

        // Joining again is answered locally.
        let (cmd, mut rx) = command(Op::Join(room.clone(), None));
        assert!(rig.client.on_command(cmd, rig.t0).is_empty());
        assert_eq!(now(&mut rx), Some(Response::Joined { room }));
    }

    #[test]
    fn the_router_spelling_of_a_room_is_the_one_held() {
        let mut rig = rig();
        rig.connect();
        let asked = repo("example-org/EXAMPLE-REPO");
        let spelled = repo("example-org/example-repo");
        let (cmd, mut rx) = command(Op::Join(asked, None));
        rig.client.on_command(cmd, rig.t0);
        rig.client.on_frame(
            ServerFrame::Joined {
                room: spelled.clone(),
            },
            rig.t0,
        );
        assert_eq!(
            now(&mut rx),
            Some(Response::Joined {
                room: spelled.clone()
            })
        );
        assert_eq!(rig.shared.lock().status().rooms[0].room, spelled);
    }

    #[test]
    fn joins_go_one_at_a_time_and_the_queue_is_bounded() {
        let mut rig = rig();
        rig.connect();
        let (first, _rx1) = command(Op::Join(ticket(1), None));
        assert_eq!(rig.client.on_command(first, rig.t0).len(), 1);
        let mut waiting = Vec::new();
        for n in 0..JOIN_QUEUE {
            let (cmd, rx) = command(Op::Join(ticket(10 + u32::try_from(n).unwrap()), None));
            assert!(
                rig.client.on_command(cmd, rig.t0).is_empty(),
                "a second join went out while one was in flight"
            );
            waiting.push(rx);
        }
        let (over, mut rx) = command(Op::Join(ticket(99), None));
        rig.client.on_command(over, rig.t0);
        assert_eq!(now(&mut rx), Some(Response::Busy {}));

        // The first answer sends the next.
        let next = rig
            .client
            .on_frame(ServerFrame::JoinRefused { room: ticket(1) }, rig.t0);
        assert_eq!(next, vec![ClientFrame::Join { room: ticket(10) }]);
    }

    #[test]
    fn a_join_the_router_never_answers_times_out_and_frees_the_queue() {
        let mut rig = rig();
        rig.connect();
        let (first, mut rx1) = command(Op::Join(ticket(1), None));
        rig.client.on_command(first, rig.t0);
        let (second, _rx2) = command(Op::Join(ticket(2), None));
        rig.client.on_command(second, rig.t0);

        assert!(rig.client.on_tick(rig.at(5)).is_empty());
        assert!(now(&mut rx1).is_none());
        let frames = rig.client.on_tick(rig.at(10));
        assert_eq!(now(&mut rx1), Some(Response::TimedOut {}));
        assert_eq!(frames, vec![ClientFrame::Join { room: ticket(2) }]);
        // The late answer is not taken as the answer to the next join.
        rig.client
            .on_frame(ServerFrame::Joined { room: ticket(1) }, rig.at(11));
        assert!(rig.shared.lock().held(&ticket(1)).is_none());
    }

    #[test]
    fn membership_survives_a_reconnect_and_is_rejoined_one_at_a_time() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        rig.join(ticket(2));

        rig.client.on_disconnect();
        assert!(rig.shared.lock().held(&ticket(1)).is_none());

        let frames = rig.connect();
        assert_eq!(
            frames,
            vec![
                ClientFrame::Join { room: ticket(1) },
                ClientFrame::Projects {}
            ]
        );
        let next = rig
            .client
            .on_frame(ServerFrame::Joined { room: ticket(1) }, rig.t0);
        assert_eq!(next, vec![ClientFrame::Join { room: ticket(2) }]);
        // A room the router no longer admits is forgotten.
        rig.client
            .on_frame(ServerFrame::JoinRefused { room: ticket(2) }, rig.t0);
        assert!(rig.shared.lock().held(&ticket(1)).is_some());
        assert!(!rig.shared.lock().remembers(&ticket(2)));
    }

    #[test]
    fn a_disconnect_answers_everything_waiting() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        let (post, mut post_rx) = command(Op::Post {
            room: ticket(1),
            kind: MessageKind::Claim,
            body: body("x"),
            consumer: None,
        });
        assert_eq!(rig.client.on_command(post, rig.t0).len(), 1);
        let (queued, mut queued_rx) = command(Op::Post {
            room: ticket(1),
            kind: MessageKind::Claim,
            body: body("y"),
            consumer: None,
        });
        assert!(rig.client.on_command(queued, rig.t0).is_empty());
        let (backfill, mut backfill_rx) = command(Op::Backfill {
            room: ticket(1),
            after: None,
        });
        rig.client.on_command(backfill, rig.t0);
        rig.client.on_disconnect();
        // The post already written may have been recorded: retrying it could
        // duplicate it, so it is not answered as if it never left.
        assert_eq!(
            now(&mut post_rx),
            Some(Response::PostUnconfirmed { room: ticket(1) })
        );
        // The one still queued here never reached the router.
        assert_eq!(now(&mut queued_rx), Some(Response::NotConnected {}));
        assert_eq!(now(&mut backfill_rx), Some(Response::NotConnected {}));

        let (late, mut rx) = command(Op::List);
        assert!(rig.client.on_command(late, rig.t0).is_empty());
        assert_eq!(now(&mut rx), Some(Response::NotConnected {}));
    }

    #[test]
    fn a_post_is_confirmed_by_its_own_delivery_and_tagged_with_its_poster() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        let me = ConsumerKey::new("session-a").unwrap();
        rig.shared
            .lock()
            .drain(&me, None, None, None, Timestamp::now());

        let (post, mut rx) = command(Op::Post {
            room: ticket(1),
            kind: MessageKind::Claim,
            body: body("taking it"),
            consumer: Some(me.clone()),
        });
        let frames = rig.client.on_command(post, rig.t0);
        assert_eq!(
            frames,
            vec![ClientFrame::Post {
                room: ticket(1),
                kind: MessageKind::Claim,
                body: body("taking it"),
            }]
        );
        assert!(now(&mut rx).is_none());

        // Somebody else's message with the same words is not the confirmation.
        rig.client.on_frame(
            ServerFrame::Message {
                room: ticket(1),
                seq: RoomSeq(6),
                from: host("shelter"),
                kind: MessageKind::Claim,
                body: body("taking it"),
            },
            rig.t0,
        );
        assert!(now(&mut rx).is_none());

        rig.client.on_frame(
            ServerFrame::Message {
                room: ticket(1),
                seq: RoomSeq(7),
                from: host("sandbank"),
                kind: MessageKind::Claim,
                body: body("taking it"),
            },
            rig.t0,
        );
        assert_eq!(
            now(&mut rx),
            Some(Response::Posted {
                room: ticket(1),
                seq: RoomSeq(7)
            })
        );
        let drained = rig
            .shared
            .lock()
            .drain(&me, None, None, None, Timestamp::now());
        assert_eq!(drained.messages.len(), 1, "own post was drained back");
        assert_eq!(drained.messages[0].seq, RoomSeq(6));
    }

    #[test]
    fn a_received_message_wakes_whoever_is_tailing() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        let mut arrivals = rig.shared.arrivals();
        arrivals.mark_unchanged();
        rig.client.on_frame(
            ServerFrame::Message {
                room: ticket(1),
                seq: RoomSeq(1),
                from: host("shelter"),
                kind: MessageKind::Ack,
                body: body("x"),
            },
            rig.t0,
        );
        assert!(
            arrivals.has_changed().unwrap(),
            "a tail waiting on this room would sleep through the message"
        );
    }

    #[test]
    fn a_post_to_a_room_not_held_never_reaches_the_router() {
        let mut rig = rig();
        rig.connect();
        let (post, mut rx) = command(Op::Post {
            room: ticket(1),
            kind: MessageKind::Ack,
            body: body("x"),
            consumer: None,
        });
        assert!(rig.client.on_command(post, rig.t0).is_empty());
        assert_eq!(now(&mut rx), Some(Response::NotMember { room: ticket(1) }));
    }

    fn post_op(room: RoomId, text: &str) -> Op {
        Op::Post {
            room,
            kind: MessageKind::Claim,
            body: body(text),
            consumer: None,
        }
    }

    fn post_frame(room: RoomId, text: &str) -> ClientFrame {
        ClientFrame::Post {
            room,
            kind: MessageKind::Claim,
            body: body(text),
        }
    }

    fn echo(room: RoomId, seq: u64, text: &str) -> ServerFrame {
        ServerFrame::Message {
            room,
            seq: RoomSeq(seq),
            from: host("sandbank"),
            kind: MessageKind::Claim,
            body: body(text),
        }
    }

    fn refused(room: RoomId) -> ServerFrame {
        ServerFrame::PostRefused {
            room,
            category: PostRefusal::RateLimited,
        }
    }

    #[test]
    fn posts_waiting_are_bounded_one_is_in_flight_per_room_and_expiry_tells_them_apart() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        rig.join(ticket(2));
        let mut waiting = Vec::new();
        for n in 0..POSTS_IN_FLIGHT - 1 {
            let (post, rx) = command(post_op(ticket(1), &format!("m{n}")));
            let frames = rig.client.on_command(post, rig.t0);
            // Only the first post in a room goes out; the rest wait here.
            assert_eq!(frames.len(), usize::from(n == 0), "post {n}: {frames:?}");
            waiting.push(rx);
        }
        // Another room has its own post in flight.
        let (other, mut other_rx) = command(post_op(ticket(2), "elsewhere"));
        assert_eq!(
            rig.client.on_command(other, rig.t0),
            vec![post_frame(ticket(2), "elsewhere")]
        );
        // Queued posts count against the cap.
        let (over, mut rx) = command(post_op(ticket(1), "over"));
        assert!(rig.client.on_command(over, rig.t0).is_empty());
        assert_eq!(now(&mut rx), Some(Response::Busy {}));

        rig.client.on_tick(rig.at(10));
        let mut answers = waiting.iter_mut().map(|rx| now(rx).unwrap());
        // Written and never answered: it may have landed.
        assert_eq!(
            answers.next(),
            Some(Response::PostUnconfirmed { room: ticket(1) })
        );
        // Never written: safe to send again.
        assert!(answers.all(|a| a == Response::TimedOut {}));
        assert_eq!(
            now(&mut other_rx),
            Some(Response::PostUnconfirmed { room: ticket(2) })
        );
    }

    #[test]
    fn a_refusal_that_overtakes_the_previous_echo_lands_on_the_right_post() {
        // On the router a refusal is written straight back while a delivered
        // post's echo goes through the room queue, so a refusal of B can
        // arrive before the echo of A. Here B is not written until A is
        // answered, so there is no B to refuse yet: whatever arrives answers
        // A, and only A.
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        let (a, mut a_rx) = command(post_op(ticket(1), "same words"));
        assert_eq!(
            rig.client.on_command(a, rig.t0),
            vec![post_frame(ticket(1), "same words")]
        );
        let (b, mut b_rx) = command(post_op(ticket(1), "same words"));
        assert!(
            rig.client.on_command(b, rig.t0).is_empty(),
            "B was written while A was in flight"
        );

        // A is delivered; B goes out only now.
        let next = rig
            .client
            .on_frame(echo(ticket(1), 7, "same words"), rig.t0);
        assert_eq!(
            now(&mut a_rx),
            Some(Response::Posted {
                room: ticket(1),
                seq: RoomSeq(7)
            })
        );
        assert_eq!(next, vec![post_frame(ticket(1), "same words")]);
        assert!(now(&mut b_rx).is_none());

        // B is refused, and the refusal is B's.
        assert!(rig.client.on_frame(refused(ticket(1)), rig.t0).is_empty());
        assert_eq!(
            now(&mut b_rx),
            Some(Response::PostRefused {
                room: ticket(1),
                category: Category::Known(PostRefusal::RateLimited)
            })
        );
    }

    #[test]
    fn a_refusal_while_the_first_post_is_in_flight_is_the_first_posts() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        let (a, mut a_rx) = command(post_op(ticket(1), "a"));
        rig.client.on_command(a, rig.t0);
        let (b, mut b_rx) = command(post_op(ticket(1), "b"));
        rig.client.on_command(b, rig.t0);

        let next = rig.client.on_frame(refused(ticket(1)), rig.t0);
        assert!(matches!(now(&mut a_rx), Some(Response::PostRefused { .. })));
        assert_eq!(next, vec![post_frame(ticket(1), "b")]);
        // A late echo of something else does not confirm B.
        rig.client.on_frame(echo(ticket(1), 3, "a"), rig.t0);
        assert!(now(&mut b_rx).is_none());
        rig.client.on_frame(echo(ticket(1), 4, "b"), rig.t0);
        assert_eq!(
            now(&mut b_rx),
            Some(Response::Posted {
                room: ticket(1),
                seq: RoomSeq(4)
            })
        );
    }

    #[test]
    fn an_echo_confirms_only_the_post_in_flight_with_the_same_kind_and_words() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        let (a, mut a_rx) = command(post_op(ticket(1), "mine"));
        rig.client.on_command(a, rig.t0);
        // Same host, same room, other words: another laptop on this login.
        rig.client.on_frame(echo(ticket(1), 1, "not mine"), rig.t0);
        // Same words, other kind.
        rig.client.on_frame(
            ServerFrame::Message {
                room: ticket(1),
                seq: RoomSeq(2),
                from: host("sandbank"),
                kind: MessageKind::Question,
                body: body("mine"),
            },
            rig.t0,
        );
        assert!(now(&mut a_rx).is_none());
    }

    #[test]
    fn a_refusal_for_another_room_answers_nothing_here() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        rig.join(ticket(2));
        let (a, mut a_rx) = command(post_op(ticket(1), "a"));
        rig.client.on_command(a, rig.t0);
        rig.client.on_frame(refused(ticket(2)), rig.t0);
        assert!(now(&mut a_rx).is_none());
        assert_eq!(rig.shared.lock().status().rooms[1].last_refusal, None);
    }

    #[test]
    fn a_refused_post_is_answered_recorded_and_carries_no_body() {
        let dir = std::env::temp_dir().join(format!("sealmesh-room-ledger-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ledger = dir.join("audit.jsonl");
        let mut rig = rig_with(Some(ledger.clone()));
        rig.connect();
        rig.join(ticket(1));
        let (post, mut rx) = command(Op::Post {
            room: ticket(1),
            kind: MessageKind::Claim,
            body: body("a distinctive sentence"),
            consumer: None,
        });
        rig.client.on_command(post, rig.t0);
        rig.client.on_frame(
            ServerFrame::PostRefused {
                room: ticket(1),
                category: PostRefusal::BudgetExhausted,
            },
            rig.t0,
        );
        assert_eq!(
            now(&mut rx),
            Some(Response::PostRefused {
                room: ticket(1),
                category: Category::Known(PostRefusal::BudgetExhausted)
            })
        );
        let text = std::fs::read_to_string(&ledger).unwrap();
        assert!(text.contains("budget_exhausted"), "{text}");
        assert!(
            !text.contains("distinctive"),
            "a body reached the ledger: {text}"
        );
        assert_eq!(
            rig.shared.lock().status().rooms[0].last_refusal,
            Some(Category::Known(PostRefusal::BudgetExhausted))
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_unknown_refusal_category_is_answered_recorded_and_counted() {
        let dir =
            std::env::temp_dir().join(format!("sealmesh-room-unknown-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ledger = dir.join("audit.jsonl");
        let mut rig = rig_with(Some(ledger.clone()));
        rig.connect();
        rig.join(ticket(1));
        let (post, mut post_rx) = command(Op::Post {
            room: ticket(1),
            kind: MessageKind::Claim,
            body: body("x"),
            consumer: None,
        });
        rig.client.on_command(post, rig.t0);
        rig.client
            .on_unknown_refusal(UnknownRefusal::Post { room: ticket(1) });
        assert_eq!(
            now(&mut post_rx),
            Some(Response::PostRefused {
                room: ticket(1),
                category: Category::UNKNOWN
            })
        );

        let (backfill, mut backfill_rx) = command(Op::Backfill {
            room: ticket(1),
            after: None,
        });
        rig.client.on_command(backfill, rig.t0);
        rig.client
            .on_unknown_refusal(UnknownRefusal::Backfill { room: ticket(1) });
        assert_eq!(
            now(&mut backfill_rx),
            Some(Response::BackfillRefused {
                room: ticket(1),
                category: Category::UNKNOWN
            })
        );

        assert_eq!(rig.shared.lock().status().unknown_refusals, 2);
        let records = audit::read_all(&ledger).unwrap();
        assert_eq!(records.len(), 2, "{records:?}");
        assert!(records.iter().all(|r| matches!(
            r,
            audit::Record::Room(
                RoomRecord::PostRefused { category, .. }
            ) if category.is_unknown()
        ) || matches!(
            r,
            audit::Record::Room(RoomRecord::BackfillRefused { category, .. }) if category.is_unknown()
        )));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_refusal_that_answers_no_post_of_ours_writes_nothing() {
        let dir =
            std::env::temp_dir().join(format!("sealmesh-room-unsolicited-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ledger = dir.join("audit.jsonl");
        let mut rig = rig_with(Some(ledger.clone()));
        rig.connect();
        rig.join(ticket(1));
        for _ in 0..100 {
            rig.client.on_frame(refused(ticket(1)), rig.t0);
            rig.client
                .on_unknown_refusal(UnknownRefusal::Post { room: ticket(1) });
            rig.client
                .on_unknown_refusal(UnknownRefusal::Backfill { room: ticket(1) });
        }
        rig.client.on_tick(rig.at(15 * 60));
        assert!(!ledger.exists(), "a router wrote to the ledger at will");
        let status = rig.shared.lock().status();
        assert_eq!(status.unknown_refusals, 0);
        assert_eq!(status.rooms[0].last_refusal, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_flood_of_refusals_writes_a_bounded_number_of_lines() {
        let dir = std::env::temp_dir().join(format!("sealmesh-room-flood-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ledger = dir.join("audit.jsonl");
        let mut rig = rig_with(Some(ledger.clone()));
        rig.connect();
        rig.join(ticket(1));
        for n in 0..1000 {
            let (post, _rx) = command(post_op(ticket(1), "x"));
            rig.client.on_command(post, rig.t0);
            rig.client.on_frame(refused(ticket(1)), rig.t0);
            // A reconnect midway does not start a fresh allowance.
            if n == 500 {
                rig.client.on_disconnect();
                rig.connect();
                rig.client
                    .on_frame(ServerFrame::Joined { room: ticket(1) }, rig.t0);
            }
        }
        let itemised = audit::read_all(&ledger).unwrap().len();
        assert_eq!(itemised, usize::try_from(REFUSAL_RECORDS).unwrap());
        rig.client.on_tick(rig.at(15 * 60));
        let records = audit::read_all(&ledger).unwrap();
        assert_eq!(
            records.len(),
            usize::try_from(REFUSAL_RECORDS).unwrap() + 1,
            "{} lines",
            records.len()
        );
        assert!(matches!(
            records.last(),
            Some(audit::Record::Room(RoomRecord::RefusalsNotItemised { refusals, .. }))
                if *refusals == 1000 - REFUSAL_RECORDS
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn received_traffic_is_counted_per_room_and_sender_and_written_per_interval() {
        let dir =
            std::env::temp_dir().join(format!("sealmesh-room-traffic-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ledger = dir.join("audit.jsonl");
        let mut rig = rig_with(Some(ledger.clone()));
        rig.connect();
        rig.join(ticket(1));
        for (n, from) in ["shelter", "shelter", "atlas"].iter().enumerate() {
            rig.client.on_frame(
                ServerFrame::Message {
                    room: ticket(1),
                    seq: RoomSeq(u64::try_from(n).unwrap()),
                    from: host(from),
                    kind: MessageKind::Claim,
                    body: body("a distinctive sentence"),
                },
                rig.t0,
            );
        }
        rig.client.on_tick(rig.at(60));
        assert!(!ledger.exists(), "written before the interval ended");
        rig.client.on_tick(rig.at(15 * 60));
        let text = std::fs::read_to_string(&ledger).unwrap();
        assert!(!text.contains("distinctive"), "{text}");
        let records = audit::read_all(&ledger).unwrap();
        let mut counts: Vec<(String, u64)> = records
            .iter()
            .filter_map(|r| match r {
                audit::Record::Room(RoomRecord::Received { from, messages, .. }) => {
                    Some((from.as_str().to_owned(), *messages))
                }
                _ => None,
            })
            .collect();
        counts.sort();
        assert_eq!(counts, [("atlas".to_owned(), 1), ("shelter".to_owned(), 2)]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn traffic_from_more_pairs_than_the_cap_is_counted_not_itemised() {
        let mut traffic = Traffic::new(Instant::now());
        for n in 0..(TRAFFIC_PAIRS + 10) {
            let from = host(&format!("h{n}"));
            traffic.count(&ticket(1), &from);
        }
        let records = traffic.records(Timestamp::now());
        assert_eq!(records.len(), TRAFFIC_PAIRS + 1);
        assert!(matches!(
            records.last(),
            Some(RoomRecord::ReceivedElsewhere { messages: 10, .. })
        ));
    }

    #[test]
    fn a_backfill_is_single_flight_and_a_late_reply_is_told_apart_by_its_cursor() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        let (first, mut rx1) = command(Op::Backfill {
            room: ticket(1),
            after: None,
        });
        assert_eq!(
            rig.client.on_command(first, rig.t0),
            vec![ClientFrame::Backfill {
                room: ticket(1),
                after: None
            }]
        );
        let (second, mut rx2) = command(Op::Backfill {
            room: ticket(1),
            after: Some(RoomSeq(5)),
        });
        assert!(rig.client.on_command(second, rig.t0).is_empty());
        assert_eq!(now(&mut rx2), Some(Response::Busy {}));

        rig.client.on_tick(rig.at(10));
        assert_eq!(now(&mut rx1), Some(Response::TimedOut {}));

        let (third, mut rx3) = command(Op::Backfill {
            room: ticket(1),
            after: Some(RoomSeq(5)),
        });
        rig.client.on_command(third, rig.at(11));
        // The late answer to the first request echoes `after: None`.
        rig.client.on_frame(
            ServerFrame::Backfill {
                room: ticket(1),
                after: None,
                messages: Vec::new(),
                next: None,
            },
            rig.at(11),
        );
        assert!(
            now(&mut rx3).is_none(),
            "a late reply answered the next request"
        );
        let entry = HistoryEntry {
            seq: RoomSeq(6),
            from: host("shelter"),
            kind: MessageKind::Ack,
            body: body("ok"),
        };
        rig.client.on_frame(
            ServerFrame::Backfill {
                room: ticket(1),
                after: Some(RoomSeq(5)),
                messages: vec![entry.clone()],
                next: None,
            },
            rig.at(11),
        );
        assert_eq!(
            now(&mut rx3),
            Some(Response::Backfill {
                room: ticket(1),
                after: Some(RoomSeq(5)),
                messages: vec![entry],
                next: None
            })
        );
    }

    #[test]
    fn a_known_backfill_refusal_is_answered() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        let (cmd, mut rx) = command(Op::Backfill {
            room: ticket(1),
            after: None,
        });
        rig.client.on_command(cmd, rig.t0);
        rig.client.on_frame(
            ServerFrame::BackfillRefused {
                room: ticket(1),
                category: BackfillRefusal::Unavailable,
            },
            rig.t0,
        );
        assert_eq!(
            now(&mut rx),
            Some(Response::BackfillRefused {
                room: ticket(1),
                category: Category::Known(BackfillRefusal::Unavailable)
            })
        );
    }

    #[test]
    fn listing_is_on_demand_and_held_to_the_cooldown() {
        let mut rig = rig();
        rig.connect();
        // Ticks alone never list.
        for s in 0..120 {
            assert!(rig.client.on_tick(rig.at(s)).is_empty());
        }
        let (first, mut rx) = command(Op::List);
        assert_eq!(
            rig.client.on_command(first, rig.t0),
            vec![ClientFrame::ListRooms {}]
        );
        let rooms = RoomList::new(vec![ticket(1)]).unwrap();
        rig.client.on_frame(
            ServerFrame::RoomList {
                rooms: rooms.clone(),
            },
            rig.at(1),
        );
        assert_eq!(
            now(&mut rx),
            Some(Response::Rooms {
                rooms: rooms.clone(),
                age_secs: 0
            })
        );

        // Within the cooldown: the previous listing, and nothing sent.
        let (again, mut rx) = command(Op::List);
        assert!(rig.client.on_command(again, rig.at(20)).is_empty());
        assert_eq!(
            now(&mut rx),
            Some(Response::Rooms {
                rooms,
                age_secs: 19
            })
        );

        // After it: the router is asked again.
        let (later, _rx) = command(Op::List);
        assert_eq!(
            rig.client.on_command(later, rig.at(30)),
            vec![ClientFrame::ListRooms {}]
        );
    }

    #[test]
    fn a_listing_that_timed_out_is_throttled_not_retried_at_once() {
        let mut rig = rig();
        rig.connect();
        let (first, mut rx) = command(Op::List);
        rig.client.on_command(first, rig.t0);
        rig.client.on_tick(rig.at(10));
        assert_eq!(now(&mut rx), Some(Response::TimedOut {}));
        let (again, mut rx) = command(Op::List);
        assert!(rig.client.on_command(again, rig.at(12)).is_empty());
        assert_eq!(
            now(&mut rx),
            Some(Response::ListThrottled {
                retry_after_secs: 18
            })
        );
    }

    #[test]
    fn the_cooldown_is_not_below_its_floor() {
        assert!(Timing::default().list_cooldown >= MIN_LIST_COOLDOWN);
        assert!(MIN_LIST_COOLDOWN >= Duration::from_secs(30));
    }

    #[test]
    fn a_cooldown_configured_below_the_floor_is_held_to_it() {
        let mut rig = rig();
        rig.client.timing.list_cooldown = Duration::ZERO;
        rig.connect();
        let (first, _rx) = command(Op::List);
        assert_eq!(
            rig.client.on_command(first, rig.t0),
            vec![ClientFrame::ListRooms {}]
        );
        rig.client.on_frame(
            ServerFrame::RoomList {
                rooms: RoomList::new(Vec::new()).unwrap(),
            },
            rig.t0,
        );
        let (again, _rx) = command(Op::List);
        assert!(
            rig.client.on_command(again, rig.at(29)).is_empty(),
            "listed again inside the floor"
        );
        let (later, _rx) = command(Op::List);
        assert_eq!(
            rig.client.on_command(later, rig.at(30)),
            vec![ClientFrame::ListRooms {}]
        );
    }

    #[test]
    fn disabled_withdraws_once_and_then_sends_nothing() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        rig.on.store(false, Ordering::SeqCst);

        let (first, mut rx) = command(Op::Join(ticket(2), None));
        assert_eq!(
            rig.client.on_command(first, rig.t0),
            vec![ClientFrame::Leave { room: ticket(1) }],
            "the held room was not left"
        );
        assert_eq!(now(&mut rx), Some(Response::Disabled {}));

        for op in [
            Op::Join(ticket(2), None),
            Op::Leave(ticket(1), None),
            Op::Post {
                room: ticket(1),
                kind: MessageKind::Ack,
                body: body("x"),
                consumer: None,
            },
            Op::Backfill {
                room: ticket(1),
                after: None,
            },
            Op::List,
        ] {
            let (cmd, mut rx) = command(op);
            assert!(rig.client.on_command(cmd, rig.t0).is_empty());
            assert_eq!(now(&mut rx), Some(Response::Disabled {}));
        }
        // Everything held is forgotten, and a message arriving is dropped.
        assert_eq!(rig.shared.lock().room_count(), 0);
        rig.client.on_frame(
            ServerFrame::Message {
                room: ticket(1),
                seq: RoomSeq(1),
                from: host("shelter"),
                kind: MessageKind::Ack,
                body: body("x"),
            },
            rig.t0,
        );
        assert_eq!(rig.shared.lock().totals(), (0, 0));
        // A reconnect while disabled re-joins nothing.
        rig.client.on_disconnect();
        assert!(rig.connect().is_empty());
        assert!(rig.client.on_tick(rig.at(1)).is_empty());
    }

    #[test]
    fn turning_chat_off_answers_what_was_waiting() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        let (cmd, mut rx) = command(Op::Backfill {
            room: ticket(1),
            after: None,
        });
        rig.client.on_command(cmd, rig.t0);
        rig.on.store(false, Ordering::SeqCst);
        assert_eq!(
            rig.client.on_tick(rig.at(1)),
            vec![ClientFrame::Leave { room: ticket(1) }]
        );
        assert_eq!(now(&mut rx), Some(Response::Disabled {}));
        assert_eq!(rig.shared.lock().room_count(), 0);
        assert!(rig.client.on_tick(rig.at(2)).is_empty());
    }

    #[test]
    fn a_room_frame_arriving_after_chat_goes_off_is_dropped_before_any_tick() {
        let dir =
            std::env::temp_dir().join(format!("sealmesh-room-offframe-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ledger = dir.join("audit.jsonl");
        let mut rig = rig_with(Some(ledger.clone()));
        rig.connect();
        rig.join(ticket(1));
        let (post, mut post_rx) = command(post_op(ticket(1), "x"));
        rig.client.on_command(post, rig.t0);

        // Switched off; the very next thing is a frame, not a tick.
        rig.on.store(false, Ordering::SeqCst);
        let frames = rig
            .client
            .on_frame(echo(ticket(1), 1, "from someone"), rig.t0);
        assert_eq!(frames, vec![ClientFrame::Leave { room: ticket(1) }]);
        assert_eq!(rig.shared.lock().totals(), (0, 0), "stored while off");
        // The post was written before chat went off, so it may have landed.
        assert_eq!(
            now(&mut post_rx),
            Some(Response::PostUnconfirmed { room: ticket(1) })
        );
        assert!(rig.client.on_frame(refused(ticket(1)), rig.t0).is_empty());
        assert!(
            rig.client
                .on_frame(echo(ticket(1), 2, "x"), rig.t0)
                .is_empty()
        );

        rig.on.store(true, Ordering::SeqCst);
        rig.client.on_tick(rig.at(15 * 60));
        assert!(
            !ledger.exists(),
            "a frame received while off reached the ledger: {:?}",
            audit::read_all(&ledger)
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_late_joined_for_a_room_not_held_is_left_at_once() {
        let mut rig = rig();
        rig.connect();
        let (cmd, mut rx) = command(Op::Join(ticket(1), None));
        rig.client.on_command(cmd, rig.t0);
        rig.client.on_tick(rig.at(10));
        assert_eq!(now(&mut rx), Some(Response::TimedOut {}));
        // The router did admit it, late.
        assert_eq!(
            rig.client
                .on_frame(ServerFrame::Joined { room: ticket(1) }, rig.at(11)),
            vec![ClientFrame::Leave { room: ticket(1) }]
        );
        assert!(!rig.shared.lock().remembers(&ticket(1)));
    }

    #[test]
    fn leaving_forgets_the_room_and_tells_the_router() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        let (cmd, mut rx) = command(Op::Leave(ticket(1), None));
        assert_eq!(
            rig.client.on_command(cmd, rig.t0),
            vec![ClientFrame::Leave { room: ticket(1) }]
        );
        assert_eq!(now(&mut rx), Some(Response::Left { room: ticket(1) }));
        let (cmd, mut rx) = command(Op::Leave(ticket(1), None));
        assert!(rig.client.on_command(cmd, rig.t0).is_empty());
        assert_eq!(now(&mut rx), Some(Response::NotMember { room: ticket(1) }));
    }

    fn chat(pid: &str) -> ConsumerKey {
        ConsumerKey::new(&format!("chat:{pid}")).unwrap()
    }

    #[test]
    fn a_named_holders_leave_keeps_a_room_an_anonymous_join_also_holds() {
        let mut rig = rig();
        rig.connect();
        // The hook or an MCP tool: an anonymous, permanent claim.
        rig.join(ticket(1));
        // A `chat` invocation joins the same room under its own claim; the
        // room is already held, so this is answered at once.
        assert_eq!(
            rig.join_as_fast(ticket(1), Some(chat("111"))),
            Response::Joined { room: ticket(1) }
        );
        let (frames, response) = rig.leave_as(ticket(1), Some(chat("111")));
        assert!(
            frames.is_empty(),
            "the anonymous claim still holds the room; the router must not be told to leave: {frames:?}"
        );
        assert_eq!(response, Response::Left { room: ticket(1) });
        assert!(
            rig.shared.lock().held(&ticket(1)).is_some(),
            "an anonymous claim must keep the room after a named holder leaves"
        );
    }

    #[test]
    fn a_named_holders_leave_forgets_a_room_only_that_holder_holds() {
        let mut rig = rig();
        rig.connect();
        rig.join_as(ticket(1), Some(chat("111")));
        let (frames, response) = rig.leave_as(ticket(1), Some(chat("111")));
        assert_eq!(frames, vec![ClientFrame::Leave { room: ticket(1) }]);
        assert_eq!(response, Response::Left { room: ticket(1) });
        assert!(!rig.shared.lock().remembers(&ticket(1)));
    }

    #[test]
    fn two_named_holders_must_both_leave_before_the_room_is_forgotten() {
        let mut rig = rig();
        rig.connect();
        rig.join_as(ticket(1), Some(chat("111")));
        assert_eq!(
            rig.join_as_fast(ticket(1), Some(chat("222"))),
            Response::Joined { room: ticket(1) }
        );
        let (frames, response) = rig.leave_as(ticket(1), Some(chat("111")));
        assert!(
            frames.is_empty(),
            "the second holder still holds the room: {frames:?}"
        );
        assert_eq!(response, Response::Left { room: ticket(1) });
        assert!(
            rig.shared.lock().held(&ticket(1)).is_some(),
            "one holder leaving must not forget a room a second holder still claims"
        );
        let (frames, response) = rig.leave_as(ticket(1), Some(chat("222")));
        assert_eq!(frames, vec![ClientFrame::Leave { room: ticket(1) }]);
        assert_eq!(response, Response::Left { room: ticket(1) });
        assert!(!rig.shared.lock().remembers(&ticket(1)));
    }

    #[test]
    fn a_reconnect_rejoins_a_room_while_any_claim_remains() {
        let mut rig = rig();
        rig.connect();
        rig.join_as(ticket(1), Some(chat("111")));

        rig.client.on_disconnect();
        assert!(rig.shared.lock().held(&ticket(1)).is_none());
        assert!(
            rig.shared.lock().remembers(&ticket(1)),
            "a claim must survive a disconnect"
        );

        let frames = rig.connect();
        assert_eq!(
            frames,
            vec![
                ClientFrame::Join { room: ticket(1) },
                ClientFrame::Projects {}
            ]
        );
        rig.client
            .on_frame(ServerFrame::Joined { room: ticket(1) }, rig.t0);
        assert!(rig.shared.lock().held(&ticket(1)).is_some());

        // The re-join claimed nothing new: the original holder is still the
        // only one, and leaving it forgets the room exactly as it would
        // have before the disconnect.
        let (frames, response) = rig.leave_as(ticket(1), Some(chat("111")));
        assert_eq!(frames, vec![ClientFrame::Leave { room: ticket(1) }]);
        assert_eq!(response, Response::Left { room: ticket(1) });
    }

    /// Independent review, probe A: a named leave during the reconnect
    /// window must drop that room's own queued re-join too, or the re-join
    /// brings the room back with no claim on it once it is eventually sent.
    #[test]
    fn probe_a_a_named_leave_during_reconnect_drops_the_rooms_queued_rejoin() {
        let mut rig = rig();
        rig.connect();
        rig.join_as(ticket(1), Some(chat("111")));
        rig.join_as(ticket(2), Some(chat("222")));

        rig.client.on_disconnect();
        assert!(rig.shared.lock().held(&ticket(1)).is_none());
        assert!(rig.shared.lock().held(&ticket(2)).is_none());

        // t1's re-join goes out first; t2's is still queued behind it.
        let frames = rig.connect();
        assert_eq!(
            frames,
            vec![
                ClientFrame::Join { room: ticket(1) },
                ClientFrame::Projects {}
            ]
        );

        // chat:222 leaves t2 before its queued re-join is ever sent.
        let (leave_frames, leave_response) = rig.leave_as(ticket(2), Some(chat("222")));
        assert!(
            leave_frames.is_empty(),
            "t2 was never held on this connection, so there is nothing to tell the router yet: \
             {leave_frames:?}"
        );
        assert_eq!(leave_response, Response::Left { room: ticket(2) });
        assert!(
            !rig.shared.lock().remembers(&ticket(2)),
            "chat:222 was t2's only claim; it must be forgotten now"
        );

        // t1's re-join confirms, which drives the next queued item, if any.
        let next = rig
            .client
            .on_frame(ServerFrame::Joined { room: ticket(1) }, rig.t0);
        assert!(
            next.is_empty(),
            "t2's queued re-join must have been dropped, not sent: {next:?}"
        );
        assert!(
            !rig.shared.lock().remembers(&ticket(2)),
            "t2 must not come back once its dropped re-join would have been processed"
        );
    }

    /// Independent review, probe B: the in-flight variant of probe A -- a
    /// named leave while that same room's own re-join is already in flight
    /// to the router.
    #[test]
    fn probe_b_a_named_leave_while_its_own_rejoin_is_in_flight_still_releases_it() {
        let mut rig = rig();
        rig.connect();
        rig.join_as(ticket(1), Some(chat("111")));

        rig.client.on_disconnect();
        let frames = rig.connect();
        assert_eq!(
            frames,
            vec![
                ClientFrame::Join { room: ticket(1) },
                ClientFrame::Projects {}
            ]
        );
        // t1's re-join is now in flight, not yet confirmed.

        let (leave_frames, leave_response) = rig.leave_as(ticket(1), Some(chat("111")));
        assert!(
            leave_frames.is_empty(),
            "not held yet on this connection: {leave_frames:?}"
        );
        assert_eq!(leave_response, Response::Left { room: ticket(1) });
        assert!(!rig.shared.lock().remembers(&ticket(1)));

        // The router now confirms the abandoned re-join anyway.
        let after_joined = rig
            .client
            .on_frame(ServerFrame::Joined { room: ticket(1) }, rig.t0);
        assert_eq!(
            after_joined,
            vec![ClientFrame::Leave { room: ticket(1) }],
            "an admit for a room a concurrent leave already forgot must be released, not \
             resurrected with no claim on it"
        );
        assert!(!rig.shared.lock().remembers(&ticket(1)));
    }

    /// Independent review, probe C: a signal (or any other reason a caller
    /// stops waiting) during a join must not strand a claim nobody will
    /// ever release.
    #[test]
    fn probe_c_a_caller_that_gives_up_during_its_own_join_does_not_strand_a_claim() {
        let mut rig = rig();
        rig.connect();
        let (cmd, rx) = command(Op::Join(ticket(1), Some(chat("111"))));
        assert_eq!(
            rig.client.on_command(cmd, rig.t0),
            vec![ClientFrame::Join { room: ticket(1) }]
        );
        drop(rx); // the caller gave up before seeing the answer

        // The caller's own cleanup: a leave for a claim that never landed.
        let (leave_frames, leave_response) = rig.leave_as(ticket(1), Some(chat("111")));
        assert!(leave_frames.is_empty());
        assert_eq!(leave_response, Response::NotMember { room: ticket(1) });

        // The router confirms the join anyway, with nobody left waiting.
        let after_joined = rig
            .client
            .on_frame(ServerFrame::Joined { room: ticket(1) }, rig.t0);
        assert_eq!(
            after_joined,
            vec![ClientFrame::Leave { room: ticket(1) }],
            "an admitted room whose only caller is already gone must be released, not held \
             forever with no claim on it"
        );
        assert!(!rig.shared.lock().remembers(&ticket(1)));
    }

    /// Independent review, probe D: named claims on one room are bounded,
    /// and refused past the bound rather than evicted.
    #[test]
    fn probe_d_named_claims_on_one_room_are_capped_not_evicted() {
        let mut rig = rig();
        rig.connect();
        rig.join_as(ticket(1), Some(chat("0")));
        // 63 more distinct named claims join the same already-held room,
        // fast-pathed, filling the cap (64 total).
        for n in 1..inbox::MAX_NAMED_CLAIMS {
            let response = rig.join_as_fast(ticket(1), Some(chat(&n.to_string())));
            assert_eq!(
                response,
                Response::Joined { room: ticket(1) },
                "claim {n} should still fit under the cap"
            );
        }
        // The next distinct named holder is refused, not evicted.
        let response = rig.join_as_fast(ticket(1), Some(chat("over-the-cap")));
        assert_eq!(response, Response::Busy {});

        // The room and every earlier claim are unaffected: leaving the very
        // first claim still leaves every other one holding the room.
        assert!(rig.shared.lock().held(&ticket(1)).is_some());
        let (frames, response) = rig.leave_as(ticket(1), Some(chat("0")));
        assert!(
            frames.is_empty(),
            "the other 63 claims still hold the room: {frames:?}"
        );
        assert_eq!(response, Response::Left { room: ticket(1) });
        assert!(
            rig.shared.lock().held(&ticket(1)).is_some(),
            "the room must still be held by the other 63 claims"
        );
    }

    /// Independent review, M10: a named leave must never cancel a
    /// *different* holder's queued join for the same room.
    #[test]
    fn a_named_leave_does_not_cancel_a_different_holders_queued_join() {
        let mut rig = rig();
        rig.connect();
        let (first, mut first_rx) = command(Op::Join(ticket(1), Some(chat("A"))));
        assert_eq!(
            rig.client.on_command(first, rig.t0),
            vec![ClientFrame::Join { room: ticket(1) }]
        );
        let (second, mut second_rx) = command(Op::Join(ticket(1), Some(chat("B"))));
        assert!(
            rig.client.on_command(second, rig.t0).is_empty(),
            "queued behind the in-flight join for the same room"
        );

        // chat:A leaves before its own join is even confirmed -- harmless,
        // and must not touch chat:B's unrelated queued join.
        let (leave_frames, leave_response) = rig.leave_as(ticket(1), Some(chat("A")));
        assert!(leave_frames.is_empty());
        assert_eq!(leave_response, Response::NotMember { room: ticket(1) });

        let next = rig
            .client
            .on_frame(ServerFrame::Joined { room: ticket(1) }, rig.t0);
        assert_eq!(
            now(&mut first_rx),
            Some(Response::Joined { room: ticket(1) })
        );
        assert_eq!(
            next,
            vec![ClientFrame::Join { room: ticket(1) }],
            "chat:B's queued join must still be sent"
        );
        rig.client
            .on_frame(ServerFrame::Joined { room: ticket(1) }, rig.t0);
        assert_eq!(
            now(&mut second_rx),
            Some(Response::Joined { room: ticket(1) }),
            "a different holder's queued join must not have been cancelled by another holder's \
             leave"
        );
    }

    /// Independent review, N1/M14: an untagged leave (`holder: None`) clears
    /// only the anonymous slot -- it must not forget a room a named claim
    /// also holds.
    #[test]
    fn an_untagged_leave_clears_only_the_anonymous_slot() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1)); // anonymous: the hook or an MCP tool
        assert_eq!(
            rig.join_as_fast(ticket(1), Some(chat("111"))),
            Response::Joined { room: ticket(1) }
        );

        let (frames, response) = rig.leave_as(ticket(1), None);
        assert!(
            frames.is_empty(),
            "chat:111's claim still holds the room: {frames:?}"
        );
        assert_eq!(response, Response::Left { room: ticket(1) });
        assert!(
            rig.shared.lock().held(&ticket(1)).is_some(),
            "a named claim must survive an untagged leave"
        );

        let (frames, response) = rig.leave_as(ticket(1), Some(chat("111")));
        assert_eq!(frames, vec![ClientFrame::Leave { room: ticket(1) }]);
        assert_eq!(response, Response::Left { room: ticket(1) });
        assert!(!rig.shared.lock().remembers(&ticket(1)));
    }

    #[test]
    fn the_router_cannot_make_the_daemon_hold_a_room_it_never_asked_for() {
        let mut rig = rig();
        rig.connect();
        assert_eq!(
            rig.client
                .on_frame(ServerFrame::Joined { room: ticket(9) }, rig.t0),
            vec![ClientFrame::Leave { room: ticket(9) }]
        );
        assert_eq!(rig.shared.lock().room_count(), 0);
    }

    #[test]
    fn a_left_from_the_router_forgets_the_room() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        rig.client
            .on_frame(ServerFrame::Left { room: ticket(1) }, rig.t0);
        assert!(!rig.shared.lock().remembers(&ticket(1)));
    }

    #[test]
    fn a_turn_cap_is_reported_and_lifted_by_a_delivered_post() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        rig.client
            .on_frame(ServerFrame::TurnCapped { room: ticket(1) }, rig.t0);
        assert!(rig.shared.lock().status().rooms[0].turn_capped);
        let (post, _rx) = command(Op::Post {
            room: ticket(1),
            kind: MessageKind::Ack,
            body: body("x"),
            consumer: None,
        });
        rig.client.on_command(post, rig.t0);
        rig.client.on_frame(
            ServerFrame::Message {
                room: ticket(1),
                seq: RoomSeq(1),
                from: host("sandbank"),
                kind: MessageKind::Ack,
                body: body("x"),
            },
            rig.t0,
        );
        assert!(!rig.shared.lock().status().rooms[0].turn_capped);
    }

    fn map_frame() -> ServerFrame {
        ServerFrame::Projects {
            projects: vec![ProjectEntry {
                id: ProjectId::try_from("example-repo").unwrap(),
                repos: vec![RepoSlug::try_from("example-org/example-repo").unwrap()],
            }],
        }
    }

    fn resolve_op(slug: &str, ticket: Option<u32>) -> Op {
        Op::Resolve {
            repo: RepoSlug::try_from(slug).unwrap(),
            ticket: ticket.map(|n| TicketId::new("ENG", n).unwrap()),
        }
    }

    fn resolved(rooms: &[RoomId]) -> Response {
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

    impl Rig {
        fn resolve(
            &mut self,
            op: Op,
            at: Instant,
        ) -> (Vec<ClientFrame>, oneshot::Receiver<Response>) {
            let (cmd, rx) = command(op);
            (self.client.on_command(cmd, at), rx)
        }
    }

    #[test]
    fn a_connection_asks_for_the_map_once_and_a_resolution_waits_for_it() {
        let mut rig = rig();
        assert_eq!(rig.connect(), vec![ClientFrame::Projects {}]);

        let (frames, mut rx) = rig.resolve(resolve_op("example-org/example-repo", Some(7)), rig.t0);
        assert!(frames.is_empty(), "asked for the map twice: {frames:?}");
        assert!(now(&mut rx).is_none(), "answered before the map came");

        rig.client.on_frame(map_frame(), rig.t0);
        assert_eq!(
            now(&mut rx),
            Some(resolved(&[
                RoomId::new(
                    ProjectId::try_from("example-repo").unwrap(),
                    Scope::Ticket(TicketId::new("ENG", 7).unwrap())
                ),
                repo("example-org/example-repo"),
            ]))
        );

        // From then on, answered at once from the map, asking nothing.
        let (frames, mut rx) = rig.resolve(resolve_op("example-org/example-repo", None), rig.t0);
        assert!(frames.is_empty());
        assert_eq!(
            now(&mut rx),
            Some(resolved(&[repo("example-org/example-repo")]))
        );
    }

    #[test]
    fn a_router_that_never_sends_the_map_answers_no_rooms_at_the_deadline() {
        let mut rig = rig();
        rig.connect();
        let (_, mut rx) = rig.resolve(resolve_op("example-org/example-repo", Some(7)), rig.t0);
        let wait = Timing::default().router_wait;
        rig.client.on_tick(rig.t0 + wait - Duration::from_millis(1));
        assert!(now(&mut rx).is_none(), "gave up before the deadline");
        rig.client.on_tick(rig.t0 + wait);
        assert_eq!(now(&mut rx), Some(resolved(&[])));

        // Within the retry interval, answered empty at once without asking.
        let later = rig.t0 + wait + Duration::from_secs(1);
        let (frames, mut rx) = rig.resolve(resolve_op("example-org/example-repo", None), later);
        assert!(frames.is_empty());
        assert_eq!(now(&mut rx), Some(resolved(&[])));

        // After it, asked again, once.
        let retry = rig.t0 + wait + MAP_RETRY;
        let (frames, mut rx) = rig.resolve(resolve_op("example-org/example-repo", None), retry);
        assert_eq!(frames, vec![ClientFrame::Projects {}]);
        assert!(now(&mut rx).is_none());
        let (frames, _rx2) = rig.resolve(resolve_op("example-org/example-repo", None), retry);
        assert!(frames.is_empty(), "asked twice while one ask was out");
        rig.client.on_frame(map_frame(), retry);
        assert_eq!(
            now(&mut rx),
            Some(resolved(&[repo("example-org/example-repo")]))
        );
    }

    #[test]
    fn no_project_an_unknown_repo_and_no_map_are_one_answer() {
        let mut known = rig();
        known.connect();
        known.client.on_frame(map_frame(), known.t0);
        let (_, mut unmapped) =
            known.resolve(resolve_op("example-org/sealmesh", Some(7)), known.t0);
        let (_, mut unknown) = known.resolve(resolve_op("someone/else", Some(7)), known.t0);

        let mut silent = rig();
        silent.connect();
        let (_, mut no_map) =
            silent.resolve(resolve_op("example-org/example-repo", Some(7)), silent.t0);
        silent.client.on_tick(silent.at(60));

        let answers = [now(&mut unmapped), now(&mut unknown), now(&mut no_map)];
        assert!(
            answers.iter().all(|a| *a == Some(resolved(&[]))),
            "{answers:?}"
        );
    }

    #[test]
    fn the_project_room_is_named_only_with_the_opt_in() {
        let project = RoomId::new(ProjectId::try_from("example-repo").unwrap(), Scope::Project);
        for on in [false, true] {
            let mut rig = rig();
            rig.client = std::mem::replace(
                &mut rig.client,
                RoomClient::new(
                    rig.shared.clone(),
                    Arc::new(|| true),
                    Timing::default(),
                    None,
                    rig.t0,
                ),
            )
            .with_project_rooms(on);
            rig.connect();
            rig.client.on_frame(map_frame(), rig.t0);
            let (_, mut rx) = rig.resolve(resolve_op("example-org/example-repo", None), rig.t0);
            let Some(Response::Resolved { rooms }) = now(&mut rx) else {
                panic!()
            };
            assert_eq!(
                rooms.iter().any(|r| r.room == project),
                on,
                "project_rooms = {on}: {rooms:?}"
            );
        }
    }

    #[test]
    fn a_disconnect_answers_a_waiting_resolution_and_the_next_connection_asks_again() {
        let mut rig = rig();
        rig.connect();
        rig.client.on_frame(map_frame(), rig.t0);
        let mut rig2 = rig;
        rig2.client.on_disconnect();
        assert_eq!(rig2.connect(), vec![ClientFrame::Projects {}]);
        let (_, mut rx) = rig2.resolve(resolve_op("example-org/example-repo", None), rig2.t0);
        assert!(
            now(&mut rx).is_none(),
            "answered from the last connection's map"
        );
        rig2.client.on_disconnect();
        assert_eq!(now(&mut rx), Some(Response::NotConnected {}));
    }

    #[test]
    fn resolutions_waiting_on_the_map_are_bounded() {
        let mut rig = rig();
        rig.connect();
        let mut waiting = Vec::new();
        for _ in 0..RESOLVE_QUEUE {
            let (_, rx) = rig.resolve(resolve_op("example-org/example-repo", None), rig.t0);
            waiting.push(rx);
        }
        let (_, mut rx) = rig.resolve(resolve_op("example-org/example-repo", None), rig.t0);
        assert_eq!(now(&mut rx), Some(Response::Busy {}));
        rig.client.on_frame(map_frame(), rig.t0);
        for mut rx in waiting {
            assert!(matches!(now(&mut rx), Some(Response::Resolved { .. })));
        }
    }

    #[test]
    fn with_chat_off_nothing_asks_for_the_map() {
        let mut rig = rig();
        rig.on.store(false, Ordering::SeqCst);
        assert!(rig.connect().is_empty());
        let (frames, mut rx) = rig.resolve(resolve_op("example-org/example-repo", None), rig.t0);
        assert!(frames.is_empty());
        assert_eq!(now(&mut rx), Some(Response::Disabled {}));
    }

    #[test]
    fn a_stale_map_is_refreshed_while_it_keeps_answering() {
        let mut rig = rig();
        rig.connect();
        rig.client.on_frame(map_frame(), rig.t0);

        let fresh = rig.t0 + MAP_REFRESH - Duration::from_secs(1);
        let (frames, mut rx) = rig.resolve(resolve_op("example-org/example-repo", None), fresh);
        assert!(frames.is_empty(), "refreshed a fresh map");
        assert_eq!(
            now(&mut rx),
            Some(resolved(&[repo("example-org/example-repo")]))
        );

        // Stale: asked for again, and the old map answers meanwhile.
        let stale = rig.t0 + MAP_REFRESH;
        let (frames, mut rx) = rig.resolve(resolve_op("example-org/example-repo", None), stale);
        assert_eq!(frames, vec![ClientFrame::Projects {}]);
        assert_eq!(
            now(&mut rx),
            Some(resolved(&[repo("example-org/example-repo")]))
        );
        let (frames, _) = rig.resolve(resolve_op("example-org/example-repo", None), stale);
        assert!(frames.is_empty(), "asked twice while one ask was out");

        // The refresh goes unanswered: the old map stays, and the next ask
        // waits for `MAP_RETRY`.
        let wait = Timing::default().router_wait;
        rig.client.on_tick(stale + wait);
        let (frames, mut rx) =
            rig.resolve(resolve_op("example-org/example-repo", None), stale + wait);
        assert!(frames.is_empty());
        assert_eq!(
            now(&mut rx),
            Some(resolved(&[repo("example-org/example-repo")]))
        );
        let again = stale + MAP_RETRY;
        let (frames, _) = rig.resolve(resolve_op("example-org/example-repo", None), again);
        assert_eq!(frames, vec![ClientFrame::Projects {}]);

        // The new map replaces the old one: here, a cut that lost the repo.
        rig.client
            .on_frame(ServerFrame::Projects { projects: vec![] }, again);
        let (frames, mut rx) = rig.resolve(resolve_op("example-org/example-repo", None), again);
        assert!(frames.is_empty());
        assert_eq!(now(&mut rx), Some(resolved(&[])));
    }

    #[test]
    fn a_map_that_lost_a_repo_to_a_slow_cut_recovers_without_a_reconnect() {
        let mut rig = rig();
        rig.connect();
        // The first cut came back empty: GitHub was slow and nothing was
        // confirmed in time.
        rig.client
            .on_frame(ServerFrame::Projects { projects: vec![] }, rig.t0);
        let (_, mut rx) = rig.resolve(resolve_op("example-org/example-repo", None), rig.t0);
        assert_eq!(now(&mut rx), Some(resolved(&[])));

        let later = rig.t0 + MAP_REFRESH;
        let (frames, _) = rig.resolve(resolve_op("example-org/example-repo", None), later);
        assert_eq!(frames, vec![ClientFrame::Projects {}]);
        rig.client.on_frame(map_frame(), later);
        let (_, mut rx) = rig.resolve(resolve_op("example-org/example-repo", None), later);
        assert_eq!(
            now(&mut rx),
            Some(resolved(&[repo("example-org/example-repo")]))
        );
    }

    #[test]
    fn a_queued_join_whose_caller_gave_up_is_never_sent() {
        let mut rig = rig();
        rig.connect();
        let (first, mut first_rx) = command(Op::Join(ticket(1), None));
        assert_eq!(
            rig.client.on_command(first, rig.t0),
            vec![ClientFrame::Join { room: ticket(1) }]
        );
        let (second, second_rx) = command(Op::Join(ticket(2), None));
        assert!(rig.client.on_command(second, rig.t0).is_empty());
        let (third, mut third_rx) = command(Op::Join(ticket(3), None));
        assert!(rig.client.on_command(third, rig.t0).is_empty());
        // The second caller was told `timed_out` and went away.
        drop(second_rx);

        let next = rig
            .client
            .on_frame(ServerFrame::Joined { room: ticket(1) }, rig.t0);
        assert_eq!(
            now(&mut first_rx),
            Some(Response::Joined { room: ticket(1) })
        );
        assert_eq!(
            next,
            vec![ClientFrame::Join { room: ticket(3) }],
            "a join nobody waits for was sent"
        );
        rig.client
            .on_frame(ServerFrame::Joined { room: ticket(3) }, rig.t0);
        assert_eq!(
            now(&mut third_rx),
            Some(Response::Joined { room: ticket(3) })
        );
        assert!(!rig.shared.lock().remembers(&ticket(2)));
    }

    #[test]
    fn a_rejoin_after_a_reconnect_is_sent_though_nobody_waits_for_it() {
        let mut rig = rig();
        rig.connect();
        rig.join(ticket(1));
        rig.client.on_disconnect();
        assert_eq!(
            rig.connect(),
            vec![
                ClientFrame::Join { room: ticket(1) },
                ClientFrame::Projects {}
            ]
        );
    }

    // XYZ-106: leases from a resolution, and refused joins remembered.

    fn session(id: &str) -> ConsumerKey {
        ConsumerKey::new(id).unwrap()
    }

    impl Rig {
        /// What a resolution's join submits for one room, at `at`: the
        /// frames it sends and its answer, the router admitting any join it
        /// sends at once.
        fn hold_at(
            &mut self,
            room: RoomId,
            holder: Option<ConsumerKey>,
            at: Instant,
        ) -> (Vec<ClientFrame>, Response) {
            let (cmd, mut rx) = command(Op::Hold(room.clone(), holder));
            let mut frames = self.client.on_command(cmd, at);
            if frames.contains(&ClientFrame::Join { room: room.clone() }) {
                assert!(now(&mut rx).is_none(), "answered before the router did");
                frames.extend(self.client.on_frame(ServerFrame::Joined { room }, at));
            }
            (frames, now(&mut rx).unwrap())
        }

        /// A plain join at `at`, as an MCP tool's `join` sends it: the frames
        /// it sends and its answer, if it was answered without the router.
        fn join_at(&mut self, room: RoomId, at: Instant) -> (Vec<ClientFrame>, Option<Response>) {
            let (cmd, mut rx) = command(Op::Join(room, None));
            let frames = self.client.on_command(cmd, at);
            (frames, now(&mut rx))
        }
    }

    #[test]
    fn a_hundred_ticket_branches_resolved_over_time_never_reach_the_room_limit() {
        // A long-running daemon whose hook resolves a new ticket branch every
        // twenty minutes, and the repo room on every one. Before leases, the
        // 32nd room answered `room_limit` for the life of the daemon.
        for holder in [Some(session("session-a")), None] {
            let mut rig = rig();
            rig.connect();
            let repo_room = repo("example-org/example-repo");
            let mut most = 0;
            for n in 1..=100u32 {
                let at = rig.at(u64::from(n) * 20 * 60);
                let (_, answer) = rig.hold_at(ticket(n), holder.clone(), at);
                assert_eq!(
                    answer,
                    Response::Joined { room: ticket(n) },
                    "ticket {n}, holder {holder:?}"
                );
                let (_, answer) = rig.hold_at(repo_room.clone(), holder.clone(), at);
                assert_eq!(
                    answer,
                    Response::Joined {
                        room: repo_room.clone()
                    }
                );
                most = most.max(rig.shared.lock().room_count());
            }
            // Only the rooms renewed within the lease are held: the repo room
            // and the last few hours' tickets, well under the cap of 32.
            let window = LEASE_IDLE.as_secs() / (20 * 60);
            assert!(
                most <= usize::try_from(window).unwrap() + 1,
                "held {most} rooms at once"
            );
            assert!(rig.shared.lock().held(&ticket(100)).is_some());
            assert!(!rig.shared.lock().remembers(&ticket(1)));
        }
    }

    #[test]
    fn a_lease_is_renewed_by_asking_again_and_let_go_once_idle() {
        let mut rig = rig();
        rig.connect();
        let idle = LEASE_IDLE.as_secs();
        let holder = Some(session("session-a"));
        rig.hold_at(ticket(1), holder.clone(), rig.t0);

        // Renewed halfway through, with no new frame to the router.
        let (frames, answer) = rig.hold_at(ticket(1), holder, rig.at(idle / 2));
        assert!(frames.is_empty(), "{frames:?}");
        assert_eq!(answer, Response::Joined { room: ticket(1) });

        assert!(rig.client.on_tick(rig.at(idle)).is_empty());
        assert!(rig.shared.lock().held(&ticket(1)).is_some());
        assert!(rig.client.on_tick(rig.at(idle / 2 + idle - 1)).is_empty());
        // Idle for the whole lease since its renewal: left, and the router told.
        assert_eq!(
            rig.client.on_tick(rig.at(idle / 2 + idle)),
            vec![ClientFrame::Leave { room: ticket(1) }]
        );
        assert!(!rig.shared.lock().remembers(&ticket(1)));
    }

    #[test]
    fn an_idle_lease_is_let_go_before_a_join_is_counted_against_the_cap() {
        // No tick in between: the join itself lets go of what has expired.
        let mut rig = rig();
        rig.connect();
        let cap = rig.shared.lock().limits().rooms;
        for n in 1..=u32::try_from(cap).unwrap() {
            rig.hold_at(ticket(n), None, rig.t0);
        }
        let (cmd, mut rx) = command(Op::Hold(ticket(1000), None));
        rig.client.on_command(cmd, rig.t0);
        assert_eq!(now(&mut rx), Some(Response::RoomLimit {}));

        let later = rig.t0 + LEASE_IDLE;
        let (frames, answer) = rig.hold_at(ticket(1000), None, later);
        assert_eq!(answer, Response::Joined { room: ticket(1000) });
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f, ClientFrame::Leave { .. }))
                .count(),
            cap
        );
        assert_eq!(rig.shared.lock().room_count(), 1);
    }

    #[test]
    fn a_lease_never_drops_a_permanent_or_named_claim_on_the_same_room() {
        let mut rig = rig();
        rig.connect();
        // An MCP tool's plain join, a `chat` invocation, and the hook's lease.
        rig.join(ticket(1));
        rig.join(ticket(2));
        rig.join_as_fast(ticket(2), Some(chat("111")));
        rig.client
            .on_command(command(Op::Leave(ticket(2), None)).0, rig.t0);
        rig.hold_at(ticket(1), Some(session("session-a")), rig.t0);
        rig.hold_at(ticket(2), None, rig.t0);

        let status = rig.shared.lock().status();
        let two = status.rooms.iter().find(|r| r.room == ticket(2)).unwrap();
        assert!(two.anonymous_claim, "the lease reads as an anonymous claim");
        assert_eq!(two.named_claims, 1);
        let one = status.rooms.iter().find(|r| r.room == ticket(1)).unwrap();
        assert!(one.anonymous_claim);
        assert_eq!(one.named_claims, 1, "the lease reads as a named claim");

        assert!(rig.client.on_tick(rig.t0 + LEASE_IDLE).is_empty());
        assert!(rig.shared.lock().held(&ticket(1)).is_some());
        assert!(rig.shared.lock().held(&ticket(2)).is_some());
        let status = rig.shared.lock().status();
        let two = status.rooms.iter().find(|r| r.room == ticket(2)).unwrap();
        assert!(!two.anonymous_claim, "the expired lease is gone");
        assert_eq!(two.named_claims, 1);

        // `chat`'s own claim is unchanged: its leave releases the room.
        let (frames, response) = rig.leave_as(ticket(2), Some(chat("111")));
        assert_eq!(response, Response::Left { room: ticket(2) });
        assert_eq!(frames, vec![ClientFrame::Leave { room: ticket(2) }]);
    }

    #[test]
    fn a_leave_naming_the_holder_releases_its_lease_at_once() {
        let mut rig = rig();
        rig.connect();
        rig.hold_at(ticket(1), Some(session("session-a")), rig.t0);
        rig.hold_at(ticket(1), Some(session("session-b")), rig.t0);
        let (frames, _) = rig.leave_as(ticket(1), Some(session("session-a")));
        assert!(frames.is_empty(), "session-b still holds it: {frames:?}");
        let (frames, response) = rig.leave_as(ticket(1), Some(session("session-b")));
        assert_eq!(response, Response::Left { room: ticket(1) });
        assert_eq!(frames, vec![ClientFrame::Leave { room: ticket(1) }]);
    }

    #[test]
    fn leases_on_one_room_are_bounded() {
        let mut rig = rig();
        rig.connect();
        for n in 0..inbox::MAX_LEASES {
            let (_, answer) = rig.hold_at(ticket(1), Some(session(&format!("s{n}"))), rig.t0);
            assert_eq!(answer, Response::Joined { room: ticket(1) });
        }
        let (frames, answer) = rig.hold_at(ticket(1), Some(session("one-more")), rig.t0);
        assert!(frames.is_empty());
        assert_eq!(answer, Response::Busy {});
        // Renewing one already held still works.
        let (_, answer) = rig.hold_at(ticket(1), Some(session("s0")), rig.t0);
        assert_eq!(answer, Response::Joined { room: ticket(1) });
    }

    fn archived(room: RoomId) -> ServerFrame {
        ServerFrame::PostRefused {
            room,
            category: PostRefusal::Archived,
        }
    }

    #[test]
    fn an_archived_refusal_lets_go_of_leases_and_leaves_a_room_nothing_else_holds() {
        let mut rig = rig();
        rig.connect();
        rig.hold_at(ticket(1), Some(session("session-a")), rig.t0);
        let (first, mut first_rx) = command(post_op(ticket(1), "one"));
        let (second, mut second_rx) = command(post_op(ticket(1), "two"));
        rig.client.on_command(first, rig.t0);
        rig.client.on_command(second, rig.t0);

        let frames = rig.client.on_frame(archived(ticket(1)), rig.t0);
        assert_eq!(frames, vec![ClientFrame::Leave { room: ticket(1) }]);
        let refused = Response::PostRefused {
            room: ticket(1),
            category: Category::Known(PostRefusal::Archived),
        };
        assert_eq!(now(&mut first_rx), Some(refused.clone()));
        assert_eq!(now(&mut second_rx), Some(refused));
        assert!(!rig.shared.lock().remembers(&ticket(1)));
    }

    #[test]
    fn an_archived_refusal_keeps_a_room_a_permanent_or_named_claim_holds() {
        let mut rig = rig();
        rig.connect();
        rig.join_as(ticket(1), Some(chat("111")));
        rig.hold_at(ticket(1), Some(session("session-a")), rig.t0);
        let (post, mut rx) = command(post_op(ticket(1), "one"));
        rig.client.on_command(post, rig.t0);
        assert!(rig.client.on_frame(archived(ticket(1)), rig.t0).is_empty());
        assert!(matches!(now(&mut rx), Some(Response::PostRefused { .. })));
        assert!(rig.shared.lock().held(&ticket(1)).is_some());
        let status = rig.shared.lock().status();
        assert_eq!(status.rooms[0].named_claims, 1, "only chat's claim is left");
    }

    #[test]
    fn a_refused_room_is_not_asked_for_again_within_the_memory_window() {
        let mut rig = rig();
        rig.connect();
        let (cmd, mut rx) = command(Op::Hold(ticket(1), Some(session("session-a"))));
        assert_eq!(
            rig.client.on_command(cmd, rig.t0),
            vec![ClientFrame::Join { room: ticket(1) }]
        );
        rig.client
            .on_frame(ServerFrame::JoinRefused { room: ticket(1) }, rig.t0);
        assert_eq!(
            now(&mut rx),
            Some(Response::JoinRefused { room: ticket(1) })
        );

        // Within the window: refused here, for a resolution and a plain join
        // alike, and nothing goes to the router.
        let within = rig.t0 + REFUSED_JOIN_MEMORY - Duration::from_secs(1);
        let (cmd, mut rx) = command(Op::Hold(ticket(1), Some(session("session-a"))));
        assert!(rig.client.on_command(cmd, within).is_empty());
        assert_eq!(
            now(&mut rx),
            Some(Response::JoinRefused { room: ticket(1) })
        );
        let (frames, answer) = rig.join_at(ticket(1), within);
        assert!(frames.is_empty(), "{frames:?}");
        assert_eq!(answer, Some(Response::JoinRefused { room: ticket(1) }));

        // Another room is unaffected.
        let (frames, _) = rig.join_at(ticket(2), within);
        assert_eq!(frames, vec![ClientFrame::Join { room: ticket(2) }]);
        rig.client
            .on_frame(ServerFrame::Joined { room: ticket(2) }, within);

        // Once the window has passed, it is asked for again, and a room the
        // router now admits is not remembered as refused.
        let after = rig.t0 + REFUSED_JOIN_MEMORY;
        let (frames, answer) = rig.join_at(ticket(1), after);
        assert_eq!(frames, vec![ClientFrame::Join { room: ticket(1) }]);
        assert!(answer.is_none());
        rig.client
            .on_frame(ServerFrame::Joined { room: ticket(1) }, after);
        assert!(rig.client.refused.is_empty());
    }

    #[test]
    fn a_join_queued_behind_a_refusal_of_its_room_is_refused_without_asking() {
        let mut rig = rig();
        rig.connect();
        let (first, mut first_rx) = command(Op::Hold(ticket(1), None));
        let (second, mut second_rx) = command(Op::Join(ticket(1), Some(chat("1"))));
        let (third, _third_rx) = command(Op::Join(ticket(2), None));
        assert_eq!(rig.client.on_command(first, rig.t0).len(), 1);
        assert!(rig.client.on_command(second, rig.t0).is_empty());
        assert!(rig.client.on_command(third, rig.t0).is_empty());
        let next = rig
            .client
            .on_frame(ServerFrame::JoinRefused { room: ticket(1) }, rig.t0);
        assert_eq!(next, vec![ClientFrame::Join { room: ticket(2) }]);
        let refused = Some(Response::JoinRefused { room: ticket(1) });
        assert_eq!(now(&mut first_rx), refused);
        assert_eq!(now(&mut second_rx), refused);
    }

    #[test]
    fn refused_joins_remembered_are_bounded_and_the_oldest_goes_first() {
        let mut rig = rig();
        rig.connect();
        for n in 1..=u32::try_from(REFUSED_JOINS).unwrap() + 1 {
            let (frames, _) = rig.join_at(ticket(n), rig.t0);
            assert_eq!(frames, vec![ClientFrame::Join { room: ticket(n) }]);
            rig.client
                .on_frame(ServerFrame::JoinRefused { room: ticket(n) }, rig.t0);
        }
        assert_eq!(rig.client.refused.len(), REFUSED_JOINS);
        let (frames, _) = rig.join_at(ticket(1), rig.t0);
        assert_eq!(frames, vec![ClientFrame::Join { room: ticket(1) }]);
    }

    #[test]
    fn a_reconnects_refused_rejoin_is_not_remembered() {
        let mut rig = rig();
        rig.connect();
        rig.hold_at(ticket(1), None, rig.t0);
        rig.client.on_disconnect();
        rig.connect();
        rig.client
            .on_frame(ServerFrame::JoinRefused { room: ticket(1) }, rig.t0);
        assert!(!rig.shared.lock().remembers(&ticket(1)));
        // The next caller's join is still put to the router.
        let (frames, _) = rig.join_at(ticket(1), rig.t0);
        assert_eq!(frames, vec![ClientFrame::Join { room: ticket(1) }]);
    }

    #[test]
    fn a_refusal_the_router_sends_unasked_is_not_remembered() {
        let mut rig = rig();
        rig.connect();
        rig.client
            .on_frame(ServerFrame::JoinRefused { room: ticket(1) }, rig.t0);
        let (frames, _) = rig.join_at(ticket(1), rig.t0);
        assert_eq!(frames, vec![ClientFrame::Join { room: ticket(1) }]);
    }

    #[test]
    fn a_lease_held_only_while_a_rejoin_is_queued_expires_without_a_frame() {
        let mut rig = rig();
        rig.connect();
        rig.hold_at(ticket(1), None, rig.t0);
        rig.hold_at(ticket(2), None, rig.t0);
        rig.client.on_disconnect();
        let frames = rig.connect();
        assert_eq!(frames[0], ClientFrame::Join { room: ticket(1) });
        // Both leases lapse while ticket 1's re-join is in flight and ticket
        // 2's is queued: ticket 2's re-join is dropped, not sent later.
        assert!(rig.client.on_tick(rig.t0 + LEASE_IDLE).is_empty());
        let next = rig
            .client
            .on_frame(ServerFrame::Joined { room: ticket(1) }, rig.t0 + LEASE_IDLE);
        assert_eq!(next, vec![ClientFrame::Leave { room: ticket(1) }]);
        assert_eq!(rig.shared.lock().room_count(), 0);
    }
}
