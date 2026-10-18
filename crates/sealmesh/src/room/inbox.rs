//! The daemon's room state that local clients read: which rooms it holds,
//! the messages received in them, and each consumer's drain cursors.
//!
//! Everything here is bounded, and every bound that drops something counts
//! what it dropped and reports it:
//!
//! - messages and bytes per room, and across every room, dropping the oldest
//!   first. A drop is counted on the room and reported to each consumer and
//!   tail that had not yet been handed the message, as `lost`;
//! - rooms held, refused beyond the cap rather than evicting one;
//! - claims on one room — named ones and leases — each refused beyond its
//!   cap rather than evicting one, and leases dropped once idle;
//! - consumers, least recently used forgotten first, counted in `status`;
//! - drains awaiting their write: at most one per consumer, held on the
//!   consumer itself, so the consumer cap bounds them too.
//!
//! Nothing here is written to disk. Membership lives in this process and is
//! re-joined after a reconnect; messages lost to a daemon restart are still in
//! the router's history and can be backfilled.
//!
//! No method takes long: each is bounded by the caps above, so holding the
//! lock for one never holds up the session loop for long, and the lock is
//! never held across an `.await`.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{Duration, Instant},
};

use jiff::{SignedDuration, Timestamp};
use sealmesh_chat_wire::{RoomId, Scope};
use sealmesh_proto::PostRefusal;
use sealmesh_wire::HostId;

use super::local::{
    ConsumerKey, Delivered, MAX_DELIVERED_BYTES, MAX_MESSAGES_PER_READ, MESSAGE_BUDGET_BYTES,
    PROTOCOL, PostCategory, RoomStatus, Status, TailCursor,
};

/// How far back a consumer seen for the first time starts: long enough that
/// a session which joined a room and then took its first turn hears what
/// arrived in between, short enough that a consumer forgotten under the cap
/// and seen again is not handed much twice.
pub const NEW_CONSUMER_WINDOW: SignedDuration = SignedDuration::from_mins(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub room_messages: usize,
    pub room_bytes: usize,
    pub total_messages: usize,
    pub total_bytes: usize,
    pub rooms: usize,
    pub consumers: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            room_messages: 256,
            room_bytes: 1024 * 1024,
            total_messages: 2048,
            total_bytes: 8 * 1024 * 1024,
            rooms: 32,
            consumers: 64,
        }
    }
}

/// Whether two room ids name one room. A repo's spelling can differ in case
/// between what a client asked for and the router's project map, and the
/// router treats those as one room; project and ticket ids have one case
/// by construction.
pub fn same_room(a: &RoomId, b: &RoomId) -> bool {
    a.project == b.project
        && match (&a.scope, &b.scope) {
            (Scope::Repo(x), Scope::Repo(y)) => x.as_str().eq_ignore_ascii_case(y.as_str()),
            (x, y) => x == y,
        }
}

#[derive(Debug, Clone)]
struct Stored {
    /// Position in this room's current life, from 1.
    ordinal: u64,
    /// Position across every room, for oldest-first eviction and drains.
    order: u64,
    delivered: Delivered,
    /// The drain key of the local client that posted it, if it said.
    origin: Option<ConsumerKey>,
    /// Bytes it encodes to in a response, which is what the caps count.
    cost: usize,
}

/// The most named claims one room may hold at once. Not a room membership
/// limit — the router decides that — only a bound on how much bookkeeping
/// one room's claim set can cost this daemon, so a caller minting an
/// unbounded number of distinct holders cannot grow one `RoomBox` without
/// limit. Refused past this, never evicted: evicting the oldest claim to
/// make room for a new one is exactly the silent-membership-loss bug this
/// tracking exists to prevent.
pub(crate) const MAX_NAMED_CLAIMS: usize = 64;

/// The most leases one room may hold at once, the anonymous one included.
/// Refused past this, never evicted, for the same reason as
/// [`MAX_NAMED_CLAIMS`].
pub(crate) const MAX_LEASES: usize = MAX_NAMED_CLAIMS;

/// Who claims a room: an anonymous/permanent claim (an MCP tool's `join`, or
/// any caller that does not distinguish itself), any number of named claims,
/// up to [`MAX_NAMED_CLAIMS`], and any number of leases, up to
/// [`MAX_LEASES`]. A room is forgotten only once all three are empty (see
/// [`State::unclaim`]).
///
/// A lease is the claim a `resolve` with `join` (or a `hold`) makes: keyed
/// by its holder, `None` for a caller that names none, and stamped with when
/// it was last asked for. One idle past the bound [`State::expire_leases`] is
/// given is dropped, so rooms resolved on the way through a checkout do not
/// sit against the room cap for the life of the daemon. A lease is separate
/// from a permanent claim of the same key: expiring one never drops the other.
#[derive(Debug, Default, Clone)]
struct Claims {
    anonymous: bool,
    named: HashSet<ConsumerKey>,
    leases: HashMap<Option<ConsumerKey>, Instant>,
}

impl Claims {
    /// Adds `holder`'s claim. `false` only when `holder` is a *new* named
    /// claim and the room is already at [`MAX_NAMED_CLAIMS`] — re-claiming a
    /// holder already present always succeeds, since it changes nothing.
    fn add(&mut self, holder: Option<&ConsumerKey>) -> bool {
        match holder {
            None => {
                self.anonymous = true;
                true
            }
            Some(key) => {
                if self.named.contains(key) {
                    return true;
                }
                if self.named.len() >= MAX_NAMED_CLAIMS {
                    return false;
                }
                self.named.insert(key.clone());
                true
            }
        }
    }

    /// Takes out or renews `holder`'s lease as of `now`. `false` only when it
    /// is a *new* lease and the room already holds [`MAX_LEASES`].
    fn lease(&mut self, holder: Option<&ConsumerKey>, now: Instant) -> bool {
        let key = holder.cloned();
        if let Some(renewed) = self.leases.get_mut(&key) {
            *renewed = (*renewed).max(now);
            return true;
        }
        if self.leases.len() >= MAX_LEASES {
            return false;
        }
        self.leases.insert(key, now);
        true
    }

    fn is_empty(&self) -> bool {
        !self.anonymous && self.named.is_empty() && self.leases.is_empty()
    }
}

/// What removing one claim did, from [`State::unclaim`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unclaim {
    /// This room is not remembered at all.
    NotMember,
    /// Another claim still holds it. Nothing else happens.
    StillClaimed,
    /// The last claim was removed and the room was held: it is forgotten, and
    /// the router must be told to leave, in its own spelling of the room.
    Forgotten(RoomId),
    /// The last claim was removed, but the room was only remembered — a
    /// re-join was still pending, or it was never re-admitted after a
    /// reconnect. It is forgotten locally; there is nothing to tell the
    /// router, since it never considered this connection a member.
    ForgottenUnheld,
}

#[derive(Debug)]
struct RoomBox {
    room: RoomId,
    generation: u64,
    held: bool,
    messages: VecDeque<Stored>,
    bytes: usize,
    next_ordinal: u64,
    dropped: u64,
    last_refusal: Option<PostCategory>,
    turn_capped: bool,
    claims: Claims,
}

impl RoomBox {
    fn first_ordinal(&self) -> u64 {
        self.messages
            .front()
            .map_or(self.next_ordinal, |stored| stored.ordinal)
    }

    /// How many messages after `cursor` were dropped before being read.
    fn lost_after(&self, cursor: u64) -> u64 {
        self.first_ordinal()
            .saturating_sub(1)
            .saturating_sub(cursor)
    }

    fn pop_oldest(&mut self) -> Option<Stored> {
        let stored = self.messages.pop_front()?;
        self.bytes = self.bytes.saturating_sub(stored.cost);
        self.dropped = self.dropped.saturating_add(1);
        Some(stored)
    }
}

#[derive(Debug)]
struct Consumer {
    /// (room generation, ordinal consumed through).
    cursors: Vec<(u64, u64)>,
    used: u64,
    registered_at: Timestamp,
    /// Whether a drain answer has reached this consumer. Until one has,
    /// every drain says `new_consumer`, so a first answer that was never
    /// written does not take that news with it.
    answered: bool,
    /// The token of the drain handed out and not yet committed or released.
    /// One at a time: a second drain while one is in flight would be handed
    /// the same messages.
    in_flight: Option<u64>,
}

/// A drain that was read but whose cursors have not moved: what
/// [`State::commit_drain`] moves them to once the answer is written, or what
/// [`State::release_drain`] lets go of when it is not. Holding one blocks
/// every other drain by the same consumer, so its holder must hand it to one
/// of those two.
#[derive(Debug, PartialEq, Eq)]
pub struct DrainTicket {
    key: ConsumerKey,
    token: u64,
    /// (room generation, ordinal the answer runs through).
    moves: Vec<(u64, u64)>,
}

/// What [`State::peek_drain`] found.
#[derive(Debug, PartialEq, Eq)]
pub enum Peeked {
    /// What to answer, and the ticket that moves the cursors once it is
    /// written.
    Ready(Drained, DrainTicket),
    /// This consumer already has a drain waiting on its write. Nothing was
    /// read and nothing moved.
    InFlight,
}

#[derive(Debug)]
pub struct State {
    limits: Limits,
    host: Option<HostId>,
    connected: bool,
    rooms: Vec<RoomBox>,
    consumers: HashMap<ConsumerKey, Consumer>,
    next_order: u64,
    next_generation: u64,
    clock: u64,
    total_messages: usize,
    total_bytes: usize,
    dropped: u64,
    unknown_refusals: u64,
    consumers_evicted: u64,
    next_drain: u64,
}

/// What `push` did with a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pushed {
    Stored,
    /// No held room matches it.
    NotHeld,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drained {
    pub messages: Vec<Delivered>,
    pub lost: u64,
    pub new_consumer: bool,
    pub more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tailed {
    pub room: RoomId,
    pub messages: Vec<Delivered>,
    pub cursor: TailCursor,
    pub lost: u64,
    pub reset: bool,
}

impl State {
    pub fn new(limits: Limits, now: Timestamp) -> Self {
        // Generations start from the clock, so a tail cursor from before a
        // daemon restart never names a room's new life.
        let seed = u64::try_from(now.as_microsecond()).unwrap_or(1).max(1);
        Self {
            limits,
            host: None,
            connected: false,
            rooms: Vec::new(),
            consumers: HashMap::new(),
            next_order: 1,
            next_generation: seed,
            clock: 0,
            total_messages: 0,
            total_bytes: 0,
            dropped: 0,
            unknown_refusals: 0,
            consumers_evicted: 0,
            next_drain: 1,
        }
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    pub fn set_connected(&mut self, host: Option<HostId>) {
        self.connected = host.is_some();
        if host.is_some() {
            self.host = host;
        }
        if !self.connected {
            for room in &mut self.rooms {
                room.held = false;
            }
        }
    }

    pub fn connected(&self) -> bool {
        self.connected
    }

    pub fn host(&self) -> Option<&HostId> {
        self.host.as_ref()
    }

    fn position(&self, room: &RoomId) -> Option<usize> {
        self.rooms.iter().position(|b| same_room(&b.room, room))
    }

    fn find(&self, room: &RoomId) -> Option<&RoomBox> {
        self.rooms.iter().find(|b| same_room(&b.room, room))
    }

    fn find_mut(&mut self, room: &RoomId) -> Option<&mut RoomBox> {
        self.rooms.iter_mut().find(|b| same_room(&b.room, room))
    }

    /// The room as this daemon holds it, if it holds it on this connection.
    pub fn held(&self, room: &RoomId) -> Option<RoomId> {
        self.find(room).filter(|b| b.held).map(|b| b.room.clone())
    }

    pub fn remembers(&self, room: &RoomId) -> bool {
        self.find(room).is_some()
    }

    pub fn room_count(&self) -> usize {
        self.rooms.len()
    }

    /// Every room held on the current connection, in the router's spelling.
    pub fn held_rooms(&self) -> Vec<RoomId> {
        self.rooms
            .iter()
            .filter(|b| b.held)
            .map(|b| b.room.clone())
            .collect()
    }

    /// Every remembered room, for re-joining after a reconnect.
    pub fn remembered(&self) -> Vec<RoomId> {
        self.rooms.iter().map(|b| b.room.clone()).collect()
    }

    /// Records that the router admitted this daemon to `room`, in the router's
    /// spelling. `false` when that would pass the room cap.
    pub fn admit(&mut self, room: RoomId) -> bool {
        if let Some(existing) = self.find_mut(&room) {
            existing.room = room;
            existing.held = true;
            return true;
        }
        if self.rooms.len() >= self.limits.rooms {
            return false;
        }
        let generation = self.next_generation;
        self.next_generation = self.next_generation.saturating_add(1);
        self.rooms.push(RoomBox {
            room,
            generation,
            held: true,
            messages: VecDeque::new(),
            bytes: 0,
            next_ordinal: 1,
            dropped: 0,
            last_refusal: None,
            turn_capped: false,
            claims: Claims::default(),
        });
        true
    }

    /// Records that `holder` (or, when `None`, an anonymous/permanent
    /// caller) claims `room`. A no-op if `room` is not remembered — a caller
    /// always calls this once admission is already known, either because
    /// `admit` just succeeded or because the room was already held. `false`
    /// only when `holder` is a *new* named claim and the room is already at
    /// [`MAX_NAMED_CLAIMS`]; the room is unaffected either way.
    pub fn claim(&mut self, room: &RoomId, holder: Option<&ConsumerKey>) -> bool {
        self.find_mut(room).is_none_or(|b| b.claims.add(holder))
    }

    /// Takes out or renews `holder`'s lease on `room` as of `now` (`None`:
    /// the lease of a caller that names no holder). A no-op if `room` is not
    /// remembered, like [`State::claim`]. `false` only when it is a *new*
    /// lease and the room already holds [`MAX_LEASES`]; the room is
    /// unaffected either way.
    pub fn lease(&mut self, room: &RoomId, holder: Option<&ConsumerKey>, now: Instant) -> bool {
        self.find_mut(room)
            .is_none_or(|b| b.claims.lease(holder, now))
    }

    /// Drops every lease last renewed `idle` or longer before `now`, and
    /// forgets each room that leaves with no claim at all. Returns what
    /// happened to each room forgotten, the same as [`State::unclaim`] would
    /// have: `Forgotten` for one held, which the router must be told about.
    /// Permanent and named claims are never touched.
    pub fn expire_leases(&mut self, now: Instant, idle: Duration) -> Vec<Unclaim> {
        let mut emptied = Vec::new();
        for b in &mut self.rooms {
            let before = b.claims.leases.len();
            b.claims
                .leases
                .retain(|_, renewed| now.saturating_duration_since(*renewed) < idle);
            if b.claims.leases.len() < before && b.claims.is_empty() {
                emptied.push((b.room.clone(), b.held));
            }
        }
        self.forget_all(emptied)
    }

    /// Drops every lease on `room`, whoever holds it: the router said the
    /// room is archived, so nothing more is happening there for a checkout
    /// to follow. Permanent and named claims keep it, as they would anyway.
    pub fn drop_leases(&mut self, room: &RoomId) -> Unclaim {
        let Some(b) = self.find_mut(room) else {
            return Unclaim::NotMember;
        };
        b.claims.leases.clear();
        if !b.claims.is_empty() {
            return Unclaim::StillClaimed;
        }
        let (room, held) = (b.room.clone(), b.held);
        self.forget_all(vec![(room, held)])
            .pop()
            .unwrap_or(Unclaim::NotMember)
    }

    fn forget_all(&mut self, rooms: Vec<(RoomId, bool)>) -> Vec<Unclaim> {
        rooms
            .into_iter()
            .map(|(room, held)| {
                self.forget(&room);
                if held {
                    Unclaim::Forgotten(room)
                } else {
                    Unclaim::ForgottenUnheld
                }
            })
            .collect()
    }

    /// Whether `room` currently has no claim at all — an admitted room
    /// nobody has registered a claim on, which must not be left to sit
    /// forgotten by everyone but the router. `true` when `room` is not
    /// remembered at all, the same fail-safe direction as an empty claim
    /// set: neither is a room worth keeping.
    pub fn claims_are_empty(&self, room: &RoomId) -> bool {
        self.find(room).is_none_or(|b| b.claims.is_empty())
    }

    /// Removes one claim on `room`.
    ///
    /// `holder: None` clears the anonymous/permanent slot; `holder:
    /// Some(key)` clears that one named claim. Either way it also clears the
    /// lease of the same key, so a holder that leaves is gone whichever way
    /// it joined. The room is forgotten only once *no* claim remains —
    /// `None` is not a special unconditional leave; nothing in this tree
    /// sends an untagged `Leave` today; a future caller that wants one leaves
    /// only its own slot, the same as a named caller does.
    pub fn unclaim(&mut self, room: &RoomId, holder: Option<&ConsumerKey>) -> Unclaim {
        let Some(b) = self.find_mut(room) else {
            return Unclaim::NotMember;
        };
        match holder {
            None => b.claims.anonymous = false,
            Some(key) => {
                b.claims.named.remove(key);
            }
        }
        b.claims.leases.remove(&holder.cloned());
        if !b.claims.is_empty() {
            return Unclaim::StillClaimed;
        }
        let was_held = b.held;
        let room = b.room.clone();
        self.forget(&room);
        if was_held {
            Unclaim::Forgotten(room)
        } else {
            Unclaim::ForgottenUnheld
        }
    }

    /// Forgets `room` and everything received in it. `true` if it was held
    /// or remembered.
    pub fn forget(&mut self, room: &RoomId) -> bool {
        let Some(index) = self.position(room) else {
            return false;
        };
        let gone = self.rooms.swap_remove(index);
        self.total_messages = self.total_messages.saturating_sub(gone.messages.len());
        self.total_bytes = self.total_bytes.saturating_sub(gone.bytes);
        for consumer in self.consumers.values_mut() {
            consumer.cursors.retain(|(g, _)| *g != gone.generation);
        }
        true
    }

    /// Forgets every room, every message and every cursor.
    pub fn clear(&mut self) {
        let rooms: Vec<RoomId> = self.remembered();
        for room in rooms {
            self.forget(&room);
        }
        self.consumers.clear();
    }

    pub fn note_refusal(&mut self, room: &RoomId, category: PostCategory) {
        if category.is_unknown() {
            self.unknown_refusals = self.unknown_refusals.saturating_add(1);
        }
        if let Some(b) = self.find_mut(room) {
            b.last_refusal = Some(category);
            if category == PostCategory::Known(PostRefusal::TurnCapped) {
                b.turn_capped = true;
            }
        }
    }

    pub fn note_unknown_refusal(&mut self) {
        self.unknown_refusals = self.unknown_refusals.saturating_add(1);
    }

    pub fn note_turn_capped(&mut self, room: &RoomId, capped: bool) {
        if let Some(b) = self.find_mut(room) {
            b.turn_capped = capped;
        }
    }

    /// Stores one received message, dropping the oldest to stay in bounds.
    /// `origin` is the drain key of the local client that posted it, if any.
    pub fn push(&mut self, mut delivered: Delivered, origin: Option<ConsumerKey>) -> Pushed {
        let order = self.next_order;
        let Some(b) = self
            .rooms
            .iter_mut()
            .find(|b| b.held && same_room(&b.room, &delivered.room))
        else {
            return Pushed::NotHeld;
        };
        delivered.room = b.room.clone();
        let cost = serde_json::to_vec(&delivered)
            .map_or(MAX_DELIVERED_BYTES, |bytes| bytes.len().saturating_add(1));
        let ordinal = b.next_ordinal;
        b.next_ordinal = b.next_ordinal.saturating_add(1);
        b.messages.push_back(Stored {
            ordinal,
            order,
            delivered,
            origin,
            cost,
        });
        b.bytes = b.bytes.saturating_add(cost);
        self.next_order = self.next_order.saturating_add(1);
        self.total_messages = self.total_messages.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(cost);

        let limits = self.limits;
        let mut dropped = 0u64;
        while b.messages.len() > limits.room_messages || b.bytes > limits.room_bytes {
            let Some(gone) = b.pop_oldest() else { break };
            self.total_messages = self.total_messages.saturating_sub(1);
            self.total_bytes = self.total_bytes.saturating_sub(gone.cost);
            dropped = dropped.saturating_add(1);
        }
        while self.total_messages > limits.total_messages || self.total_bytes > limits.total_bytes {
            let oldest = self
                .rooms
                .iter_mut()
                .filter(|b| !b.messages.is_empty())
                .min_by_key(|b| b.messages.front().map_or(u64::MAX, |s| s.order));
            let Some(gone) = oldest.and_then(RoomBox::pop_oldest) else {
                break;
            };
            self.total_messages = self.total_messages.saturating_sub(1);
            self.total_bytes = self.total_bytes.saturating_sub(gone.cost);
            dropped = dropped.saturating_add(1);
        }
        self.dropped = self.dropped.saturating_add(dropped);
        Pushed::Stored
    }

    fn touch(&mut self, key: &ConsumerKey, now: Timestamp) -> bool {
        self.clock = self.clock.saturating_add(1);
        let clock = self.clock;
        if let Some(consumer) = self.consumers.get_mut(key) {
            consumer.used = clock;
            return false;
        }
        while self.consumers.len() >= self.limits.consumers.max(1) {
            // A consumer with a drain in flight goes last: forgetting it
            // would let a second drain by that key read what the first is
            // still writing.
            let Some(oldest) = self
                .consumers
                .iter()
                .min_by_key(|(_, c)| (c.in_flight.is_some(), c.used))
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            self.consumers.remove(&oldest);
            self.consumers_evicted = self.consumers_evicted.saturating_add(1);
        }
        self.consumers.insert(
            key.clone(),
            Consumer {
                cursors: Vec::new(),
                used: clock,
                registered_at: now,
                answered: false,
                in_flight: None,
            },
        );
        true
    }

    /// Where `consumer` has read `b` through. A consumer with no cursor for a
    /// room starts `NEW_CONSUMER_WINDOW` before it was first seen.
    fn cursor_for(consumer: &Consumer, b: &RoomBox) -> u64 {
        if let Some((_, through)) = consumer.cursors.iter().find(|(g, _)| *g == b.generation) {
            return *through;
        }
        let since = consumer
            .registered_at
            .checked_sub(NEW_CONSUMER_WINDOW)
            .unwrap_or(consumer.registered_at);
        b.messages
            .iter()
            .take_while(|s| s.delivered.received_at < since)
            .last()
            .map_or_else(|| b.first_ordinal().saturating_sub(1), |s| s.ordinal)
    }

    /// Hands `key` what it has not been handed, oldest first, within the caps,
    /// and moves its cursors past what it was handed — both at once, for a
    /// caller with nothing to write in between. The socket server peeks and
    /// commits separately, so its cursors move only once the answer is out.
    #[cfg(test)]
    pub fn drain(
        &mut self,
        key: &ConsumerKey,
        room: Option<&RoomId>,
        max_messages: Option<u16>,
        max_bytes: Option<u32>,
        now: Timestamp,
    ) -> Drained {
        match self.peek_drain(key, room, max_messages, max_bytes, now) {
            Peeked::Ready(drained, ticket) => {
                self.commit_drain(ticket);
                drained
            }
            Peeked::InFlight => Drained {
                messages: Vec::new(),
                lost: 0,
                new_consumer: false,
                more: true,
            },
        }
    }

    /// Reads what `key` has not been handed, oldest first, within the caps,
    /// without moving its cursors. The returned ticket moves them through
    /// exactly what was read, with [`State::commit_drain`], once the answer
    /// is written; until it is committed or released, another drain by `key`
    /// is [`Peeked::InFlight`].
    ///
    /// A consumer seen for the first time is registered here rather than at
    /// the commit, so the window it starts from is anchored to its first ask
    /// even when that first answer never arrives.
    pub fn peek_drain(
        &mut self,
        key: &ConsumerKey,
        room: Option<&RoomId>,
        max_messages: Option<u16>,
        max_bytes: Option<u32>,
        now: Timestamp,
    ) -> Peeked {
        if self
            .consumers
            .get(key)
            .is_some_and(|c| c.in_flight.is_some())
        {
            return Peeked::InFlight;
        }
        self.touch(key, now);
        let token = self.next_drain;
        self.next_drain = self.next_drain.wrapping_add(1);
        let max_messages = usize::from(
            max_messages
                .unwrap_or(MAX_MESSAGES_PER_READ)
                .clamp(1, MAX_MESSAGES_PER_READ),
        );
        let max_bytes = max_bytes
            .map_or(MESSAGE_BUDGET_BYTES, |b| {
                usize::try_from(b).unwrap_or(usize::MAX)
            })
            .clamp(1, MESSAGE_BUDGET_BYTES);

        let Some(consumer) = self.consumers.get(key) else {
            // `touch` always leaves `key` registered; this is only the
            // no-panic path (rule 39). No in-flight mark, so nothing to hold.
            return Peeked::Ready(
                Drained {
                    messages: Vec::new(),
                    lost: 0,
                    new_consumer: true,
                    more: false,
                },
                DrainTicket {
                    key: key.clone(),
                    token,
                    moves: Vec::new(),
                },
            );
        };
        let new_consumer = !consumer.answered;

        let mut lost = 0u64;
        // (room index, ordinal the cursor starts from).
        let mut starts: Vec<(usize, u64)> = Vec::new();
        let mut candidates: Vec<(u64, usize, &Stored)> = Vec::new();
        for (index, b) in self.rooms.iter().enumerate() {
            if room.is_some_and(|r| !same_room(&b.room, r)) {
                continue;
            }
            let cursor = Self::cursor_for(consumer, b);
            lost = lost.saturating_add(b.lost_after(cursor));
            let start = cursor.max(b.first_ordinal().saturating_sub(1));
            starts.push((index, start));
            for stored in b.messages.iter().filter(|s| s.ordinal > start) {
                candidates.push((stored.order, index, stored));
            }
        }
        candidates.sort_by_key(|(order, _, _)| *order);

        let mut messages = Vec::new();
        let mut bytes = 0usize;
        let mut advanced: Vec<(usize, u64)> = starts;
        let mut more = false;
        for (_, index, stored) in candidates {
            let own = stored.origin.as_ref() == Some(key);
            if !own {
                // The first message is handed out whatever it costs, so a
                // message larger than the cap cannot wedge this consumer.
                let over_bytes =
                    !messages.is_empty() && bytes.saturating_add(stored.cost) > max_bytes;
                if messages.len() >= max_messages || over_bytes {
                    more = true;
                    break;
                }
                bytes = bytes.saturating_add(stored.cost);
                messages.push(stored.delivered.clone());
            }
            if let Some(entry) = advanced.iter_mut().find(|(i, _)| *i == index) {
                entry.1 = stored.ordinal;
            }
        }

        let moves: Vec<(u64, u64)> = advanced
            .into_iter()
            .filter_map(|(index, through)| self.rooms.get(index).map(|b| (b.generation, through)))
            .collect();
        if let Some(consumer) = self.consumers.get_mut(key) {
            consumer.in_flight = Some(token);
        }

        Peeked::Ready(
            Drained {
                messages,
                lost,
                new_consumer,
                more,
            },
            DrainTicket {
                key: key.clone(),
                token,
                moves,
            },
        )
    }

    /// Moves `ticket`'s consumer past what its drain handed out: the answer
    /// was written. A cursor only moves forward; a room forgotten since the
    /// peek is skipped rather than given a cursor again; and a ticket whose
    /// consumer was forgotten meanwhile moves nothing, since that consumer
    /// is new again and starts from its window.
    pub fn commit_drain(&mut self, ticket: DrainTicket) {
        let DrainTicket { key, token, moves } = ticket;
        let live: Vec<(u64, u64)> = moves
            .into_iter()
            .filter(|(generation, _)| self.rooms.iter().any(|b| b.generation == *generation))
            .collect();
        let Some(consumer) = self.consumers.get_mut(&key) else {
            return;
        };
        if consumer.in_flight != Some(token) {
            return;
        }
        consumer.in_flight = None;
        consumer.answered = true;
        for (generation, through) in live {
            match consumer.cursors.iter_mut().find(|(g, _)| *g == generation) {
                Some(entry) => entry.1 = entry.1.max(through),
                None => consumer.cursors.push((generation, through)),
            }
        }
    }

    /// Lets go of `ticket` without moving anything: the answer was not
    /// written, so the next drain by its consumer is handed the same
    /// messages, and the same `lost`, again.
    pub fn release_drain(&mut self, ticket: &DrainTicket) {
        if let Some(consumer) = self.consumers.get_mut(&ticket.key)
            && consumer.in_flight == Some(ticket.token)
        {
            consumer.in_flight = None;
        }
    }

    /// Reads one room after `after` without consuming anything. `None` when
    /// this daemon does not remember the room.
    pub fn tail(&self, room: &RoomId, after: Option<TailCursor>) -> Option<Tailed> {
        let b = self.find(room)?;
        let (from, reset) = match after {
            Some(cursor) if cursor.generation == b.generation => (cursor.ordinal, false),
            Some(_) => (0, true),
            None => (0, false),
        };
        let lost = if from == 0 { 0 } else { b.lost_after(from) };
        let mut messages = Vec::new();
        let mut bytes = 0usize;
        let mut through = from.max(b.first_ordinal().saturating_sub(1));
        for stored in b.messages.iter().filter(|s| s.ordinal > from) {
            if messages.len() >= usize::from(MAX_MESSAGES_PER_READ)
                || bytes.saturating_add(stored.cost) > MESSAGE_BUDGET_BYTES
            {
                break;
            }
            bytes = bytes.saturating_add(stored.cost);
            messages.push(stored.delivered.clone());
            through = stored.ordinal;
        }
        Some(Tailed {
            room: b.room.clone(),
            messages,
            cursor: TailCursor {
                generation: b.generation,
                ordinal: through,
            },
            lost,
            reset,
        })
    }

    pub fn status(&self) -> Status {
        Status {
            protocol: PROTOCOL,
            connected: self.connected,
            host: self.host.clone(),
            rooms: self
                .rooms
                .iter()
                .map(|b| RoomStatus {
                    room: b.room.clone(),
                    held: b.held,
                    messages: b.messages.len(),
                    dropped: b.dropped,
                    last_refusal: b.last_refusal,
                    turn_capped: b.turn_capped,
                    // A lease reads as the claim of the same key: the shape
                    // of `status` is what older clients decode.
                    anonymous_claim: b.claims.anonymous || b.claims.leases.contains_key(&None),
                    named_claims: b.claims.named.len().saturating_add(
                        b.claims
                            .leases
                            .keys()
                            .flatten()
                            .filter(|key| !b.claims.named.contains(*key))
                            .count(),
                    ),
                })
                .collect(),
            dropped: self.dropped,
            unknown_refusals: self.unknown_refusals,
            consumers: self.consumers.len(),
            consumers_evicted: self.consumers_evicted,
        }
    }

    pub fn totals(&self) -> (usize, usize) {
        (self.total_messages, self.total_bytes)
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

    use sealmesh_chat_wire::{Body, MessageKind};
    use sealmesh_proto::RoomSeq;
    use sealmesh_wire::{ProjectId, RepoSlug, TicketId};

    use super::*;

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

    fn host(name: &str) -> HostId {
        HostId::try_from(name).unwrap()
    }

    fn key(name: &str) -> ConsumerKey {
        ConsumerKey::new(name).unwrap()
    }

    fn t0() -> Timestamp {
        "2026-09-18T12:00:00Z".parse().unwrap()
    }

    fn state(limits: Limits) -> State {
        State::new(limits, t0())
    }

    fn put(state: &mut State, room: &RoomId, n: u64, text: &str) {
        put_at(state, room, n, text, t0());
    }

    fn put_at(state: &mut State, room: &RoomId, n: u64, text: &str, at: Timestamp) {
        assert_eq!(
            state.push(message(room, n, "shelter", text, at), None),
            Pushed::Stored
        );
    }

    fn message(room: &RoomId, n: u64, from: &str, text: &str, at: Timestamp) -> Delivered {
        Delivered {
            room: room.clone(),
            seq: RoomSeq(n),
            from: host(from),
            kind: MessageKind::Claim,
            body: Body::new(text).unwrap(),
            received_at: at,
        }
    }

    fn bodies(messages: &[Delivered]) -> Vec<String> {
        messages
            .iter()
            .map(|m| m.body.as_str().to_owned())
            .collect()
    }

    #[test]
    fn a_repo_room_matches_across_case_and_nothing_else_does() {
        assert!(same_room(
            &repo("example-org/Example-repo"),
            &repo("example-org/example-repo")
        ));
        assert!(!same_room(
            &repo("example-org/example-repo"),
            &repo("example-org/other")
        ));
        assert!(!same_room(&repo("example-org/example-repo"), &ticket(1)));
        assert!(same_room(&ticket(1), &ticket(1)));
        assert!(!same_room(&ticket(1), &ticket(2)));
    }

    #[test]
    fn a_message_for_a_room_not_held_is_not_stored() {
        let mut s = state(Limits::default());
        let pushed = s.push(message(&ticket(1), 1, "shelter", "ok", t0()), None);
        assert_eq!(pushed, Pushed::NotHeld);
        assert_eq!(s.totals(), (0, 0));
    }

    #[test]
    fn each_consumer_is_handed_each_message_once() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        assert!(s.admit(room.clone()));
        let (a, b) = (key("session-a"), key("session-b"));
        // Both register before anything arrives.
        s.drain(&a, None, None, None, t0());
        s.drain(&b, None, None, None, t0());

        put(&mut s, &room, 1, "one");
        put(&mut s, &room, 2, "two");

        let first = s.drain(&a, None, None, None, t0());
        assert_eq!(bodies(&first.messages), ["one", "two"]);
        assert_eq!(first.lost, 0);
        assert!(!first.new_consumer);
        assert!(s.drain(&a, None, None, None, t0()).messages.is_empty());

        // Another consumer is unaffected by the first one's drain.
        assert_eq!(
            bodies(&s.drain(&b, None, None, None, t0()).messages),
            ["one", "two"]
        );

        put(&mut s, &room, 3, "three");
        assert_eq!(
            bodies(&s.drain(&a, None, None, None, t0()).messages),
            ["three"]
        );
        assert_eq!(
            bodies(&s.drain(&b, None, None, None, t0()).messages),
            ["three"]
        );
    }

    #[test]
    fn a_drain_stopped_by_its_cap_resumes_where_it_stopped() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        let a = key("a");
        s.drain(&a, None, None, None, t0());
        for n in 1..=5 {
            put(&mut s, &room, n, &format!("m{n}"));
        }
        let first = s.drain(&a, None, Some(2), None, t0());
        assert_eq!(bodies(&first.messages), ["m1", "m2"]);
        assert!(first.more);
        let rest = s.drain(&a, None, Some(10), None, t0());
        assert_eq!(bodies(&rest.messages), ["m3", "m4", "m5"]);
        assert!(!rest.more);
    }

    #[test]
    fn a_drain_across_rooms_is_oldest_first_and_a_room_filter_leaves_the_rest() {
        let mut s = state(Limits::default());
        let (one, two) = (ticket(1), ticket(2));
        s.admit(one.clone());
        s.admit(two.clone());
        let a = key("a");
        s.drain(&a, None, None, None, t0());
        put(&mut s, &one, 1, "1a");
        put(&mut s, &two, 1, "2a");
        put(&mut s, &one, 2, "1b");

        assert_eq!(
            bodies(&s.drain(&a, Some(&two), None, None, t0()).messages),
            ["2a"]
        );
        assert_eq!(
            bodies(&s.drain(&a, None, None, None, t0()).messages),
            ["1a", "1b"]
        );
    }

    #[test]
    fn a_full_room_drops_its_oldest_and_counts_the_loss() {
        let limits = Limits {
            room_messages: 3,
            ..Limits::default()
        };
        let mut s = state(limits);
        let room = ticket(1);
        s.admit(room.clone());
        let a = key("a");
        s.drain(&a, None, None, None, t0());
        for n in 1..=5 {
            put(&mut s, &room, n, &format!("m{n}"));
        }
        assert_eq!(s.totals().0, 3);
        let status = s.status();
        assert_eq!(status.rooms[0].dropped, 2);
        assert_eq!(status.dropped, 2);

        let drained = s.drain(&a, None, None, None, t0());
        assert_eq!(bodies(&drained.messages), ["m3", "m4", "m5"]);
        assert_eq!(drained.lost, 2, "the drop was not reported to the consumer");
        // Reported once, not on every drain after.
        assert_eq!(s.drain(&a, None, None, None, t0()).lost, 0);
    }

    #[test]
    fn a_room_over_its_byte_cap_drops_its_oldest() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        put(&mut s, &room, 1, "x");
        let one = s.totals().1;
        let limits = Limits {
            room_bytes: one * 2,
            ..Limits::default()
        };
        let mut s = state(limits);
        s.admit(room.clone());
        for n in 1..=4 {
            put(&mut s, &room, n, "x");
        }
        assert!(s.totals().1 <= one * 2, "{:?}", s.totals());
        assert_eq!(s.status().rooms[0].dropped, 2);
    }

    #[test]
    fn the_overall_cap_drops_the_oldest_message_in_any_room() {
        let limits = Limits {
            total_messages: 3,
            ..Limits::default()
        };
        let mut s = state(limits);
        let (one, two) = (ticket(1), ticket(2));
        s.admit(one.clone());
        s.admit(two.clone());
        let a = key("a");
        s.drain(&a, None, None, None, t0());
        put(&mut s, &one, 1, "1a");
        put(&mut s, &two, 1, "2a");
        put(&mut s, &two, 2, "2b");
        put(&mut s, &two, 3, "2c");

        assert_eq!(s.totals().0, 3);
        let status = s.status();
        let dropped: Vec<u64> = status.rooms.iter().map(|r| r.dropped).collect();
        assert_eq!(
            dropped,
            [1, 0],
            "the oldest overall was not the one dropped"
        );
        let drained = s.drain(&a, None, None, None, t0());
        assert_eq!(bodies(&drained.messages), ["2a", "2b", "2c"]);
        assert_eq!(drained.lost, 1);
    }

    #[test]
    fn the_overall_byte_cap_holds() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        put(&mut s, &room, 1, "x");
        let one = s.totals().1;
        let limits = Limits {
            total_bytes: one * 3,
            ..Limits::default()
        };
        let mut s = state(limits);
        s.admit(ticket(1));
        s.admit(ticket(2));
        for n in 1..=4 {
            put(&mut s, &ticket(1), n, "x");
            put(&mut s, &ticket(2), n, "x");
        }
        assert!(s.totals().1 <= one * 3);
        assert_eq!(s.status().dropped, 5);
    }

    #[test]
    fn a_consumer_seen_first_starts_ten_minutes_back() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        let now = t0();
        put_at(&mut s, &room, 1, "old", now - SignedDuration::from_mins(30));
        put_at(
            &mut s,
            &room,
            2,
            "recent",
            now - SignedDuration::from_mins(2),
        );

        let first = s.drain(&key("fresh"), None, None, None, now);
        assert!(first.new_consumer);
        assert_eq!(bodies(&first.messages), ["recent"]);
        assert_eq!(first.lost, 0, "skipping the window is not a loss");
    }

    #[test]
    fn a_room_joined_after_a_consumer_registered_is_read_from_its_start() {
        // The window is measured from when the consumer was first seen, not
        // from when it next drains: an agent idle for an hour still hears
        // everything that arrived in a room joined meanwhile.
        let mut s = state(Limits::default());
        let a = key("a");
        s.drain(&a, None, None, None, t0());
        let room = ticket(1);
        s.admit(room.clone());
        put_at(
            &mut s,
            &room,
            1,
            "hello",
            t0() + SignedDuration::from_mins(1),
        );
        let later = t0() + SignedDuration::from_hours(1);
        let drained = s.drain(&a, None, None, None, later);
        assert!(!drained.new_consumer);
        assert_eq!(bodies(&drained.messages), ["hello"]);
    }

    #[test]
    fn consumers_are_capped_least_recently_used_first() {
        let limits = Limits {
            consumers: 2,
            ..Limits::default()
        };
        let mut s = state(limits);
        s.drain(&key("a"), None, None, None, t0());
        s.drain(&key("b"), None, None, None, t0());
        s.drain(&key("a"), None, None, None, t0());
        s.drain(&key("c"), None, None, None, t0());
        let status = s.status();
        assert_eq!(status.consumers, 2);
        assert_eq!(status.consumers_evicted, 1);
        assert!(
            !s.drain(&key("a"), None, None, None, t0()).new_consumer,
            "a was evicted"
        );
        assert!(
            s.drain(&key("b"), None, None, None, t0()).new_consumer,
            "b was kept"
        );
    }

    #[test]
    fn a_consumer_is_not_handed_its_own_post() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        let (me, other) = (key("me"), key("other"));
        s.drain(&me, None, None, None, t0());
        s.drain(&other, None, None, None, t0());
        s.push(
            message(&room, 1, "sandbank", "mine", t0()),
            Some(me.clone()),
        );
        put(&mut s, &room, 2, "theirs");
        assert_eq!(
            bodies(&s.drain(&me, None, None, None, t0()).messages),
            ["theirs"]
        );
        assert_eq!(
            bodies(&s.drain(&other, None, None, None, t0()).messages),
            ["mine", "theirs"]
        );
    }

    fn peek(s: &mut State, a: &ConsumerKey) -> (Drained, DrainTicket) {
        match s.peek_drain(a, None, None, None, t0()) {
            Peeked::Ready(drained, ticket) => (drained, ticket),
            Peeked::InFlight => panic!("in flight"),
        }
    }

    #[test]
    fn a_released_drain_moves_nothing_and_is_handed_out_again() {
        let limits = Limits {
            room_messages: 2,
            ..Limits::default()
        };
        let mut s = state(limits);
        let room = ticket(1);
        s.admit(room.clone());
        let (a, fresh) = (key("a"), key("fresh"));
        s.drain(&a, None, None, None, t0());
        for n in 1..=3 {
            put(&mut s, &room, n, &format!("m{n}"));
        }
        let (first, ticket) = peek(&mut s, &a);
        assert_eq!(bodies(&first.messages), ["m2", "m3"]);
        assert_eq!(first.lost, 1);
        s.release_drain(&ticket);

        // Same messages and the same loss: nothing reached it.
        let again = s.drain(&a, None, None, None, t0());
        assert_eq!(again, first);
        // That one was written, so none of it is handed out again, nor is
        // the loss reported twice.
        let after = s.drain(&a, None, None, None, t0());
        assert!(after.messages.is_empty());
        assert_eq!(after.lost, 0);

        // A first answer that was never written leaves the consumer new.
        let (unsent, ticket) = peek(&mut s, &fresh);
        assert!(unsent.new_consumer);
        s.release_drain(&ticket);
        let sent = s.drain(&fresh, None, None, None, t0());
        assert_eq!(sent, unsent);
        assert!(!s.drain(&fresh, None, None, None, t0()).new_consumer);
    }

    #[test]
    fn a_consumer_has_one_drain_in_flight_at_a_time() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        let (a, b) = (key("a"), key("b"));
        s.drain(&a, None, None, None, t0());
        put(&mut s, &room, 1, "one");
        let (first, ticket) = peek(&mut s, &a);
        assert_eq!(bodies(&first.messages), ["one"]);
        assert_eq!(
            s.peek_drain(&a, None, None, None, t0()),
            Peeked::InFlight,
            "a second drain would hand out the same message"
        );
        // Another consumer is not held up by it.
        assert_eq!(
            bodies(&s.drain(&b, None, None, None, t0()).messages),
            ["one"]
        );
        put(&mut s, &room, 2, "two");
        s.commit_drain(ticket);
        assert_eq!(
            bodies(&s.drain(&a, None, None, None, t0()).messages),
            ["two"]
        );
    }

    #[test]
    fn a_stale_ticket_neither_commits_nor_releases_a_newer_drain() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        let a = key("a");
        s.drain(&a, None, None, None, t0());
        put(&mut s, &room, 1, "one");
        let (_, old) = peek(&mut s, &a);
        s.release_drain(&old);
        let (_, current) = peek(&mut s, &a);
        let old_copy = DrainTicket {
            key: old.key.clone(),
            token: old.token,
            moves: old.moves.clone(),
        };
        s.release_drain(&old);
        assert_eq!(s.peek_drain(&a, None, None, None, t0()), Peeked::InFlight);
        s.commit_drain(old_copy);
        assert_eq!(s.peek_drain(&a, None, None, None, t0()), Peeked::InFlight);
        s.release_drain(&current);
        assert_eq!(
            bodies(&s.drain(&a, None, None, None, t0()).messages),
            ["one"]
        );
    }

    #[test]
    fn a_commit_after_its_room_was_forgotten_moves_no_cursor_for_it() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        let a = key("a");
        s.drain(&a, None, None, None, t0());
        put(&mut s, &room, 1, "one");
        let (_, ticket) = peek(&mut s, &a);
        s.forget(&room);
        s.commit_drain(ticket);
        assert!(s.consumers.get(&a).unwrap().cursors.is_empty());
        assert!(s.consumers.get(&a).unwrap().in_flight.is_none());
    }

    #[test]
    fn a_consumer_with_a_drain_in_flight_is_evicted_last() {
        let limits = Limits {
            consumers: 2,
            ..Limits::default()
        };
        let mut s = state(limits);
        let (a, b, c) = (key("a"), key("b"), key("c"));
        let (_, ticket) = peek(&mut s, &a);
        s.drain(&b, None, None, None, t0());
        // `a` is least recently used, but its drain is still being written.
        s.drain(&c, None, None, None, t0());
        assert!(s.consumers.contains_key(&a));
        assert!(!s.consumers.contains_key(&b));
        s.commit_drain(ticket);
        let kept = s.consumers.get(&a).unwrap();
        assert!(kept.answered && kept.in_flight.is_none());
    }

    #[test]
    fn the_room_cap_refuses_rather_than_evicting() {
        let limits = Limits {
            rooms: 1,
            ..Limits::default()
        };
        let mut s = state(limits);
        assert!(s.admit(ticket(1)));
        assert!(!s.admit(ticket(2)));
        assert!(
            s.admit(ticket(1)),
            "re-admitting a held room is not a new room"
        );
        assert_eq!(s.room_count(), 1);
    }

    #[test]
    fn a_tail_consumes_nothing_and_continues_from_its_cursor() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        put(&mut s, &room, 1, "one");
        let first = s.tail(&room, None).unwrap();
        assert_eq!(bodies(&first.messages), ["one"]);
        put(&mut s, &room, 2, "two");
        let next = s.tail(&room, Some(first.cursor)).unwrap();
        assert_eq!(bodies(&next.messages), ["two"]);
        // A drain after the tails still gets both.
        let a = key("a");
        let drained = s.drain(&a, None, None, None, t0());
        assert_eq!(bodies(&drained.messages), ["one", "two"]);
    }

    #[test]
    fn a_tail_cursor_from_an_earlier_life_of_the_room_resets() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        put(&mut s, &room, 1, "one");
        put(&mut s, &room, 2, "two");
        let old = s.tail(&room, None).unwrap().cursor;
        s.forget(&room);
        s.admit(room.clone());
        put(&mut s, &room, 3, "three");
        let tailed = s.tail(&room, Some(old)).unwrap();
        assert!(tailed.reset);
        assert_eq!(bodies(&tailed.messages), ["three"]);
    }

    #[test]
    fn a_tail_reports_what_was_dropped_under_it() {
        let limits = Limits {
            room_messages: 2,
            ..Limits::default()
        };
        let mut s = state(limits);
        let room = ticket(1);
        s.admit(room.clone());
        put(&mut s, &room, 1, "one");
        let cursor = s.tail(&room, None).unwrap().cursor;
        for n in 2..=5 {
            put(&mut s, &room, n, "x");
        }
        let tailed = s.tail(&room, Some(cursor)).unwrap();
        assert_eq!(tailed.lost, 2);
        assert_eq!(tailed.messages.len(), 2);
    }

    #[test]
    fn forgetting_a_room_releases_its_messages_and_cursors() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        let a = key("a");
        s.drain(&a, None, None, None, t0());
        put(&mut s, &room, 1, "one");
        assert!(s.forget(&room));
        assert_eq!(s.totals(), (0, 0));
        assert!(!s.forget(&room));
        assert!(s.consumers.get(&a).unwrap().cursors.is_empty());
    }

    #[test]
    fn a_disconnect_keeps_membership_but_not_held() {
        let mut s = state(Limits::default());
        s.set_connected(Some(host("sandbank")));
        s.admit(ticket(1));
        assert!(s.held(&ticket(1)).is_some());
        s.set_connected(None);
        assert!(s.held(&ticket(1)).is_none());
        assert!(s.remembers(&ticket(1)));
        assert_eq!(s.remembered(), vec![ticket(1)]);
        assert_eq!(s.host().unwrap().as_str(), "sandbank");
    }

    fn largest(s: &mut State, room: &RoomId, n: u64) {
        let big = "\u{1}".repeat(sealmesh_chat_wire::MAX_BODY_CHARS);
        put(s, room, n, &big);
    }

    fn costs(s: &State) -> Vec<usize> {
        s.rooms[0].messages.iter().map(|m| m.cost).collect()
    }

    #[test]
    fn a_small_ask_gets_no_more_than_it_asked_for() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        let a = key("a");
        s.drain(&a, None, None, None, t0());
        for n in 1..=40 {
            put(&mut s, &room, n, &format!("small message {n:02}"));
        }
        let costs = costs(&s);
        // A few KiB, as the hook asks per turn: filled, not overfilled, and
        // not raised to some floor.
        let drained = s.drain(&a, None, None, Some(1024), t0());
        let total: usize = costs.iter().take(drained.messages.len()).sum();
        assert!(total <= 1024, "{total} bytes for a 1024-byte ask");
        let next = costs.get(drained.messages.len()).copied().unwrap();
        assert!(total + next > 1024, "stopped short: {total} + {next}");
        assert!(drained.more);
    }

    #[test]
    fn a_drain_may_ask_for_one_byte_and_gets_one_message() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        let a = key("a");
        s.drain(&a, None, None, None, t0());
        largest(&mut s, &room, 1);
        put(&mut s, &room, 2, "small");
        put(&mut s, &room, 3, "small");
        // Each ask is under every message's cost: one message each time,
        // whatever its size, and the rest wait behind `more`.
        for (ask, want, more) in [
            (Some(1), RoomSeq(1), true),
            (Some(0), RoomSeq(2), true),
            (Some(1), RoomSeq(3), false),
        ] {
            let drained = s.drain(&a, None, None, ask, t0());
            assert_eq!(
                drained.messages.iter().map(|m| m.seq).collect::<Vec<_>>(),
                [want],
                "{ask:?}"
            );
            assert_eq!(drained.more, more);
        }
        assert!(s.drain(&a, None, None, Some(1), t0()).messages.is_empty());
    }

    #[test]
    fn a_drain_stops_at_the_first_message_that_does_not_fit_rather_than_skipping_it() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        let a = key("a");
        s.drain(&a, None, None, None, t0());
        largest(&mut s, &room, 1);
        largest(&mut s, &room, 2);
        put(&mut s, &room, 3, "small");
        let cap = costs(&s)[0] + costs(&s)[2];
        // The small third message would fit beside the first; handing it out
        // would move the cursor past the second, which would then be lost.
        let first = s.drain(&a, None, None, Some(u32::try_from(cap).unwrap()), t0());
        assert_eq!(
            first.messages.iter().map(|m| m.seq).collect::<Vec<_>>(),
            [RoomSeq(1)]
        );
        assert!(first.more);
        let rest = s.drain(&a, None, None, None, t0());
        assert_eq!(
            rest.messages.iter().map(|m| m.seq).collect::<Vec<_>>(),
            [RoomSeq(2), RoomSeq(3)]
        );
    }

    #[test]
    fn a_small_cap_leaves_what_it_does_not_return_undrained() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        let (a, b) = (key("a"), key("b"));
        s.drain(&a, None, None, None, t0());
        s.drain(&b, None, None, None, t0());
        for n in 1..=4 {
            largest(&mut s, &room, n);
        }
        let mut seen = Vec::new();
        // Bounded, so a drain that stops handing anything out fails here
        // rather than spinning.
        for _ in 0..8 {
            let drained = s.drain(&a, None, None, Some(1), t0());
            assert!(
                !drained.messages.is_empty(),
                "a waiting message was withheld"
            );
            seen.extend(drained.messages.iter().map(|m| m.seq));
            assert_eq!(drained.lost, 0);
            if !drained.more {
                break;
            }
        }
        assert_eq!(seen, [RoomSeq(1), RoomSeq(2), RoomSeq(3), RoomSeq(4)]);
        // Another consumer's cursor is untouched by all of that.
        assert_eq!(s.drain(&b, None, None, None, t0()).messages.len(), 4);
    }

    #[test]
    fn the_largest_message_costs_no_more_than_the_fallback() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        largest(&mut s, &room, 1);
        assert!(costs(&s)[0] <= MAX_DELIVERED_BYTES, "{:?}", costs(&s));
    }

    #[test]
    fn a_drain_response_never_passes_the_message_budget() {
        let mut s = state(Limits::default());
        let room = ticket(1);
        s.admit(room.clone());
        let a = key("a");
        s.drain(&a, None, None, None, t0());
        // Four bytes a character: the widest a stored body can be, since a
        // `Body` replaces a control character before it is stored.
        // Sixty-four of them pass the message budget and still fit the room.
        let big = "😀".repeat(sealmesh_chat_wire::MAX_BODY_CHARS);
        for n in 1..=64 {
            put(&mut s, &room, n, &big);
        }
        let drained = s.drain(&a, None, None, None, t0());
        let response = super::super::local::Response::Messages {
            messages: drained.messages,
            lost: drained.lost,
            new_consumer: drained.new_consumer,
            more: drained.more,
        };
        let line = super::super::local::encode(&response, usize::MAX).unwrap();
        assert!(
            line.len() <= super::super::local::MAX_RESPONSE_BYTES,
            "{}",
            line.len()
        );
        assert!(drained.more);
    }
}
