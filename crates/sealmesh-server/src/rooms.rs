//! Room membership and live fan-out.
//!
//! A daemon joins a room over the socket it already holds, and a message
//! posted to that room goes out on every member's socket. Delivery is live:
//! a message reaches whoever is in the room at that moment. The history store
//! (`crate::history`) keeps a copy for 7 days so a late joiner can catch up.
//!
//! # One way in
//!
//! [`Membership::join`] is the only function that adds a member, and the first
//! thing it does after the join rate limit is call [`authorise_join`]. The
//! insert itself is private to this module and runs only on that function's
//! grant.
//!
//! A repo in no project therefore gets no room. `authorise_join` refuses it,
//! and a room key is only ever built from the router's project map
//! ([`canonical`]), which has no entry for it. There is no per-repo fallback
//! room, and nothing here can create one.
//!
//! # One key per room
//!
//! `authorise_join` matches a repo case-insensitively, so
//! `Example-org/Example-repo` and `example-org/example-repo` both pass. Keying on the
//! room as a client spelled it would split one conversation into two rooms,
//! so every key — on join, post and leave — is rewritten to the map's own
//! spelling first. Project ids and ticket ids have one spelling already.
//!
//! # Delivery never waits on a member
//!
//! Each connection has its own room queue, separate from the query outbox. A
//! post hands each member a copy with `try_send`, under one short lock. A
//! member whose queue is full misses that message — counted, and logged when
//! it disconnects — rather than slowing delivery to anyone else or growing
//! memory. The socket write that drains a queue is bounded by the router's
//! write timeout, so a stalled peer is disconnected, logged and taken out of
//! its rooms rather than held.
//!
//! A message still queued when its member leaves the room, or is removed by a
//! re-check, is not sent: [`Membership::next_delivery`] hands the socket only
//! messages for rooms the connection still holds, posted after its current
//! join.
//!
//! # A check never holds the connection
//!
//! A join and a re-check both wait on GitHub, for up to [`JOIN_BUDGET`]. The
//! router polls that wait inside the connection's own loop, beside reading
//! answers and draining both queues, so a slow GitHub cannot make a machine
//! read as unreachable to `who`. One check runs at a time per connection: a
//! join that arrives while one is running is refused at once, and a re-check
//! that falls due starts when the running check finishes.
//!
//! # Caps
//!
//! See each constant for its number and why. Together they bound what one
//! connection can make the router hold: [`MAX_ROOMS_PER_CONNECTION`] rooms,
//! each at most [`MAX_MEMBERS_PER_ROOM`] members, and [`ROOM_QUEUE`] undelivered
//! messages. A room with no members is forgotten at once, so the room table is
//! bounded by connections times the per-connection cap.
//!
//! # Access is re-checked, not granted for the life of a socket
//!
//! A join is decided once, and a daemon's socket can stay open for days. So
//! every [`REVALIDATE_EVERY`] each connection runs `authorise_join` again for
//! every room it holds, with its own credential, and leaves any room it no
//! longer passes — including one the check could not finish in time, which is
//! the fail-closed direction. Each connection's first re-check is offset by a
//! jitter of up to half a period, so the reconnect after a deploy does not
//! become a synchronised burst of GitHub calls every period. A revoked read
//! permission therefore keeps someone in a room for at most one and a half
//! intervals, plus up to one [`JOIN_BUDGET`] waiting behind a join check that
//! was already running when the re-check fell due, plus the re-check's own
//! [`JOIN_BUDGET`], plus the access cache's TTL. Re-checking every recipient on every
//! post was the alternative, and lost: it multiplies GitHub calls by room size
//! on each message, and the router would have to hold every member's
//! credential in a shared structure to do it.
//!
//! # In-process, like the connection registry
//!
//! Membership lives in this router process and nowhere else. A deploy empties
//! every room, which is the same trade `registry` makes for connections, and
//! the reason `desired_count` stays 1: a second task would hold a second,
//! disjoint set of rooms.
//!
//! # Persisted before it is delivered
//!
//! A post that passes every limit is written to the room's history
//! ([`crate::history`]) **before** it is put on anyone's queue, and a write
//! that fails refuses the post as `PostRefusal::NotRecorded` rather than
//! delivering it. A room exists so somebody can join late and read what
//! happened, so a message the history does not have is one that, for anyone
//! not connected at that instant, never happened — and an agent can retry a
//! refusal where it cannot discover a gap. The stated cost is that an
//! unreachable table stops posting.
//!
//! The whole of seed, reserve, persist and deliver runs under the room's own
//! ingest lock, so the order messages reach sockets is the order the table has
//! them: without it, two posts whose writes completed out of order would
//! arrive as `seq` 6 before 5. The invariant is per room — `seq` is a room's
//! counter, and nothing orders one room's messages against another's — so the
//! lock is too. It used to be one lock for the whole router, which bought
//! nothing more and let one room's slow write, or a seed retry against an
//! unreachable table, hold up posting in every other room.
//!
//! The locks live in their own table keyed by the room's canonical key, not
//! on [`Room`], because a room is forgotten the moment it empties: a post
//! whose write is still in flight when its room empties and re-forms must
//! still hold up the re-formed room's first post, whose seed would otherwise
//! read the table before that write lands and number over it. An entry exists
//! only while a post holds or waits on it and is removed by the last one to
//! let go, so the table holds at most one entry per post in flight — one per
//! daemon connection, since each posts one at a time — and never more than
//! [`MAX_INGEST_ROOMS`], past which a post is refused rather than queued
//! (rule 44).
//!
//! **Keeping that wait off a connection's critical path is the caller's.**
//! [`Membership::post`] is a future, and `http::serve_daemon` pins it into the
//! connection's `select!` rather than awaiting it in an arm body, with the
//! posts behind it in a queue one burst wide. A connection whose write is slow
//! has to go on answering a `Query` and draining its room queue — otherwise a
//! chat outage reads as every posting machine being unreachable to `who`,
//! which is the same invariant the join check is pinned into a branch for.
//!
//! A room whose seed read fails, or whose write fails because the table could
//! not be reached, refuses posts for [`STORE_BACKOFF`] before trying the store
//! again. Without that, an unreachable table would make every post pay the
//! whole [`PERSIST_BUDGET`] with the room's ingest lock held, and every post
//! behind it wait for that in turn. The backoff is checked before the lock is
//! taken, so a refusal inside the window waits on nothing, and again once it
//! is held, for the post that queued behind the one that failed. A write
//! refused as a sequence collision starts no backoff: the next post takes the
//! next number and lands, which is the point of burning the number.
//!
//! A sequence number is taken before the write, so a refused write leaves its
//! number unused: numbering is monotonic but **not** gap-free. The number is
//! deliberately not rolled back. A write whose response was lost but which
//! did land would then collide forever and wedge the room, where burning the
//! number lets the next post through and leaves only a gap.
//!
//! # What a post may cost
//!
//! A member's post passes three limits before it takes a sequence number or
//! is cloned into anyone's queue. First the room's turn cap ([`turn_cap`]):
//! two hosts who have taken the most turns it allows there, with no third
//! host posting, are both refused, and the one who did not post is sent
//! `ServerFrame::TurnCapped` once. A post it refuses spends nothing at all.
//! Then its host's post rate, then that host's token allowance for the room,
//! both in [`room_budget`] with their numbers. A post the allowance refuses
//! has still spent its place in the rate. A refused post delivers nothing,
//! takes no sequence number, and is answered with `ServerFrame::PostRefused`.
//!
//! [`turn_cap`]: crate::turn_cap
//!
//! # An archived ticket room refuses writes before any limit
//!
//! A ticket room whose pull request has merged is archived ([`archive`]), and
//! a post to it is refused as `PostRefusal::Archived` before the turn cap, the
//! post rate or the allowance is consulted — so an archived room spends
//! nothing. Joining it and reading its history still work; nothing is deleted.
//! The merge is settled with GitHub **before** the ingest lock is taken, in
//! [`Membership::post`], with the posting connection's own token; the ingest
//! path only reads the verdict already recorded. A GitHub call under that
//! lock would stall posting in the room for as long as it took. The verdict
//! is read before the room is seeded, so a post to an archived room never
//! makes the router read the store.
//!
//! [`archive`]: crate::archive
//!
//! # What is logged
//!
//! A refused join, a lapsed membership, and the first refused post of each
//! run of refusals, with the host, the room's project and its scope's kind
//! (rule 28). A store failure is logged when it happens and starts a
//! [`STORE_BACKOFF`] window whose refusals are not logged again, so an outage
//! costs one line per room per window rather than one per post. Never the
//! token, never a message body, and never the repo or ticket a refused
//! request named.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use sealmesh_chat_wire::{Body, MessageKind, RoomId, Scope};
use sealmesh_proto::{BackfillRefusal, PostRefusal, RoomList, RoomSeq, SecretToken, ServerFrame};
use sealmesh_wire::HostId;
use tokio::sync::mpsc;

use crate::{
    access_cache,
    archive::{self, Recorded, Verdicts},
    auth::Directory,
    check_budget::CheckBudgets,
    history::{self, HistoryError, Record, RoomHistory},
    projects::ProjectMap,
    room_access::authorise_join,
    room_budget::{self, Allowances, Overspent, PostRates, RateSpend, Spend, TokensPerHour},
    room_reads,
    turn_cap::{Turn, TurnCap, TurnRuns},
};

/// Longest one history read or write may take before the post that triggered
/// it is refused.
///
/// It bounds two things at once: how long a post waits on the store, and — as
/// the room's ingest lock is held throughout — how long one slow write can
/// hold up the next post to the same room. A refusal an agent can retry is a
/// better answer than a socket that has stopped responding.
pub const PERSIST_BUDGET: Duration = Duration::from_secs(5);

/// How long a room whose store read or write could not be made refuses posts
/// before asking the store again.
///
/// The seed read and the write both run while the room's ingest lock is held.
/// Without remembering a failure, *every* later post to the room would try
/// again and pay the whole [`PERSIST_BUDGET`] with that lock held — an
/// unreachable table would then not merely stop posting, it would queue every
/// post to the room behind a five-second timeout, repeatedly, with no backoff.
///
/// Thirty seconds bounds that to one attempt per room per thirty seconds, a
/// sixfold amortisation of the budget, and bounds recovery to half a minute
/// once the table answers again. Posts in the meantime are refused
/// immediately, without taking the lock, and are retryable, so the waiting is
/// paid by the poster rather than by everyone queued behind it. Longer would
/// leave a recovered table looking broken for no gain; shorter would let the
/// retries dominate the lock again.
pub const STORE_BACKOFF: Duration = Duration::from_secs(30);

/// Most rooms that may have a post holding or waiting on their ingest lock at
/// once (rule 44).
///
/// A lock-table entry exists only while a post holds or waits on it, and a
/// daemon connection runs one post at a time, so the table is bounded by the
/// connections posting right now. This is the ceiling under that argument
/// rather than a number it is expected to reach: the same 1,024 the archive
/// verdict table holds, far past the engineers this router serves. A post
/// that would add an entry past it is refused as not recorded — retryable,
/// and the fail-closed direction — rather than waiting for room.
pub const MAX_INGEST_ROOMS: usize = 1024;

/// The wall-clock second every test post is stamped with, so a stored
/// `expires_at` is a value a test can predict.
///
/// Computed once per test binary, a minute behind the real clock, rather
/// than a fixed epoch second (XYZ-108, worker contract rule 4): the read
/// path now filters on `expires_at`, so a fixed historical constant reads as
/// already expired against a real clock advanced far enough past it, which a
/// fixture must not depend on. Memoized so every call in one test run agrees
/// on the same second, which is what keeps an equality assertion against it
/// exact.
#[cfg(test)]
fn test_posted_at() -> u64 {
    static POSTED_AT: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *POSTED_AT.get_or_init(|| {
        crate::history::now_epoch()
            .unwrap_or(1_700_000_000)
            .saturating_sub(60)
    })
}

/// Most rooms one connection may hold at once.
///
/// A daemon joins a repo room and a ticket room for each checkout it is
/// working in, and a project room when someone opts in. Sixteen covers an
/// engineer with several checkouts on different tickets across a few projects.
/// It is also the bound on one connection's re-check: at most sixteen
/// `authorise_join` calls per [`REVALIDATE_EVERY`].
pub const MAX_ROOMS_PER_CONNECTION: usize = 16;

/// Most members one room may hold.
///
/// A member is a connected machine. Sixty-four is several times the number of
/// engineers this router serves, and it bounds the cost of one post: sixty-four
/// copies of a message frame that is at most about 24 KiB (a 4,000-character
/// body of worst-case JSON escapes), roughly 1.5 MiB of queued frames.
pub const MAX_MEMBERS_PER_ROOM: usize = 64;

/// Room messages one connection may have queued and not yet written.
///
/// Thirty-two worst-case message frames is under 800 KiB held for a stalled
/// member before it starts missing messages. A live socket drains far faster
/// than agents post, so a member that fills this is one that has stopped
/// reading.
pub const ROOM_QUEUE: usize = 32;

/// Most join attempts one connection may make per [`JOIN_WINDOW`] (rule 23).
///
/// A join costs up to one GitHub call per repo in the room's project, made
/// with the caller's token. Sixteen a minute lets a reconnecting daemon fill
/// every one of its [`MAX_ROOMS_PER_CONNECTION`] slots at once, and no faster.
/// A join past the limit is refused before GitHub is asked anything.
pub const MAX_JOINS_PER_WINDOW: u32 = 16;

/// The fixed window [`MAX_JOINS_PER_WINDOW`] is counted over.
pub const JOIN_WINDOW: Duration = Duration::from_secs(60);

/// Wall-clock budget for one join's access check, and for one connection's
/// whole re-check. The same ten seconds a query gives its access checks
/// (`query::ACCESS_BUDGET`). A check still running at the deadline refuses,
/// the fail-closed side. The wait itself never holds the connection: the
/// router polls it beside reading answers and draining queues.
pub const JOIN_BUDGET: Duration = Duration::from_secs(10);

/// How often each connection re-checks the rooms it holds. Tied to the access
/// cache's ceiling: re-checking more often than a cached answer can change
/// would spend a lock and a hash per room to learn nothing new.
pub const REVALIDATE_EVERY: Duration = access_cache::MAX_TTL;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ConnectionId(u64);

/// A fixed-window count of join attempts.
struct JoinWindow {
    started: Instant,
    attempts: u32,
}

impl JoinWindow {
    fn new(now: Instant) -> Self {
        Self {
            started: now,
            attempts: 0,
        }
    }

    fn allow(&mut self, now: Instant) -> bool {
        if now.saturating_duration_since(self.started) >= JOIN_WINDOW {
            self.started = now;
            self.attempts = 0;
        }
        if self.attempts >= MAX_JOINS_PER_WINDOW {
            return false;
        }
        self.attempts = self.attempts.saturating_add(1);
        true
    }
}

/// One queued room message, and when it was posted.
///
/// `stamp` comes from one counter for the whole router process that only ever
/// goes up. A room that empties is forgotten, so when it re-forms its wire
/// `seq` starts over from 1 — but its stamps never do, which is what lets a
/// message from the room's previous life be told apart from one posted since
/// the connection's current join.
pub struct Delivery {
    stamp: u64,
    frame: ServerFrame,
}

struct Connection {
    /// The host this connection's credential resolved to, so a turn-cap
    /// notice can find the other participant's connections in a room.
    host: HostId,
    outbox: mpsc::Sender<Delivery>,
    /// Every room this connection holds, with the post stamp current at the
    /// moment it joined. A queued message is handed to the socket only while
    /// the connection still holds its room and the message's stamp is later
    /// than that join — so nothing queued before a leave, a revocation, a
    /// leave-and-rejoin or the room re-forming arrives after it. A stamp and
    /// not the room's `seq`, because the room's counter restarts when the
    /// room re-forms and the stamp does not.
    rooms: BTreeMap<RoomId, u64>,
    joins: JoinWindow,
    undelivered: u64,
}

#[derive(Default)]
struct Room {
    members: BTreeSet<ConnectionId>,
    last_seq: u64,
    /// Whether this room has learned where its stored numbering left off.
    ///
    /// A room is forgotten when it empties, so a re-formed room is a fresh
    /// `Room` with `last_seq` at 0 — which is exactly the case the XYZ-59
    /// review recorded, where the first post after a re-form would reuse a
    /// number the table already holds. The first post into an unseeded room
    /// reads the room's highest persisted sequence number and continues from
    /// it, so the counter is seeded from the table and never from 1. A
    /// restarted router is the same case.
    seeded: bool,
    /// When this room's last seed read or write could not be made, if one
    /// could not.
    ///
    /// A failed seed leaves the room unseeded, and an unseeded room must not
    /// take a number; a failed write means the table is not answering. Without
    /// remembering either, every later post would try the store again and pay
    /// [`PERSIST_BUDGET`] under the room's ingest lock. Posts within
    /// [`STORE_BACKOFF`] of this are refused without touching the store or
    /// the lock.
    store_failed_at: Option<Instant>,
}

/// One room's ingest lock, and how many posts hold or wait on it.
struct IngestSlot {
    lock: Arc<tokio::sync::Mutex<()>>,
    users: usize,
}

/// Every room's ingest lock, keyed by the room's canonical key. See the
/// module docs for why this is its own table rather than a field of [`Room`].
///
/// An entry is added by the first [`IngestLease`] for its room and removed by
/// the last one dropped, so an entry never outlives the posts using it. The
/// map's own lock is a plain mutex held for a lookup and never across an
/// await, and is never taken while `inner` is held.
#[derive(Default)]
struct IngestLocks {
    slots: Mutex<HashMap<RoomId, IngestSlot>>,
}

impl IngestLocks {
    fn slots(&self) -> MutexGuard<'_, HashMap<RoomId, IngestSlot>> {
        // Nothing under this lock can panic, so a poisoned map is consistent.
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A claim on `room`'s lock, or `None` when the table already holds
    /// [`MAX_INGEST_ROOMS`] other rooms. Taking the lock itself is
    /// [`IngestLease::acquire`], which is where a post waits.
    fn lease(&self, room: &RoomId) -> Option<IngestLease<'_>> {
        let mut slots = self.slots();
        let lock = match slots.get_mut(room) {
            Some(slot) => {
                slot.users = slot.users.saturating_add(1);
                Arc::clone(&slot.lock)
            }
            None => {
                if slots.len() >= MAX_INGEST_ROOMS {
                    return None;
                }
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                slots.insert(
                    room.clone(),
                    IngestSlot {
                        lock: Arc::clone(&lock),
                        users: 1,
                    },
                );
                lock
            }
        };
        Some(IngestLease {
            locks: self,
            room: room.clone(),
            lock,
        })
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.slots().len()
    }
}

/// A post's claim on one room's ingest lock, held from before it waits until
/// after it lets go. Dropping it — including dropping a post future still
/// waiting for the lock — gives the claim back and removes the room's entry
/// when it was the last.
struct IngestLease<'a> {
    locks: &'a IngestLocks,
    room: RoomId,
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl<'a> IngestLease<'a> {
    /// Waits for the room's lock. Fair: posts to one room take it in the
    /// order they asked, which is what `seq` order rests on.
    async fn acquire(self) -> IngestGuard<'a> {
        let held = Arc::clone(&self.lock).lock_owned().await;
        IngestGuard {
            _held: held,
            _lease: self,
        }
    }
}

impl Drop for IngestLease<'_> {
    fn drop(&mut self) {
        let mut slots = self.locks.slots();
        if let Some(slot) = slots.get_mut(&self.room) {
            slot.users = slot.users.saturating_sub(1);
            if slot.users == 0 {
                slots.remove(&self.room);
            }
        }
    }
}

/// A held room ingest lock. Fields drop in declaration order, so the lock is
/// released before the lease gives its claim back.
struct IngestGuard<'a> {
    _held: tokio::sync::OwnedMutexGuard<()>,
    _lease: IngestLease<'a>,
}

#[derive(Default)]
struct Inner {
    connections: HashMap<ConnectionId, Connection>,
    rooms: HashMap<RoomId, Room>,
    /// Every host's token allowance per room. Outlives rooms and connections,
    /// so neither a room re-forming nor a reconnect resets one.
    allowances: Allowances,
    /// Every host's post rate, keyed the same way and for the same reason.
    post_rates: PostRates,
    /// Every room's current run of posts between at most two hosts, keyed
    /// by the room's canonical key. Outlives rooms and connections, so
    /// neither a re-form, a rejoin nor a reconnect resets one.
    turns: TurnRuns,
    /// Every ticket room's merge verdict, keyed the same way and outliving
    /// rooms for the same reason: a room that empties and re-forms is still
    /// archived.
    archive: Verdicts,
}

/// Every room and who is in it, for this router process.
///
/// There is deliberately no `Default`: a store has to be chosen. A defaulted
/// one would either be a client nobody configured or a sink that discards
/// messages, and the second is exactly the failure this design exists to make
/// impossible — a router that delivers happily and records nothing.
pub struct Rooms {
    inner: Mutex<Inner>,
    next_connection: AtomicU64,
    /// The last stamp given to a posted message, in any room. Read by `admit`
    /// and advanced by `post`, both while holding `inner`, so a join and a
    /// post are ordered against each other exactly as they happened.
    last_stamp: AtomicU64,
    /// Where a room's messages are kept. See the module docs on persisting
    /// before delivering.
    history: Arc<dyn RoomHistory>,
    /// One lock per room, each held for the whole of one post to that room:
    /// the seed, the limit checks, the sequence number, the history write and
    /// the fan-out. Async locks because they are held across the store.
    ///
    /// This is what makes delivery order and stored order the same. It is not
    /// protecting `Inner` — `inner` does that — it is serialising *ingest*
    /// into one room, so that no two posts to it can interleave between
    /// taking a number and being delivered.
    ingest: IngestLocks,
    /// Longest one history read or write may take. [`PERSIST_BUDGET`] unless a
    /// test shortens it.
    persist_budget: Duration,
    /// Longest one merge check may take. [`archive::CHECK_BUDGET`] unless a
    /// test shortens it.
    merge_budget: Duration,
}

impl Rooms {
    pub fn new(history: Arc<dyn RoomHistory>) -> Self {
        Self::with_limits(TokensPerHour::DEFAULT, TurnCap::DEFAULT, history)
    }

    /// A registry whose hosts each have `per_hour` tokens per room, and whose
    /// two-host exchanges are capped at `turns` posts. The post rate is not
    /// configurable.
    pub fn with_limits(
        per_hour: TokensPerHour,
        turns: TurnCap,
        history: Arc<dyn RoomHistory>,
    ) -> Self {
        Self {
            inner: Mutex::new(Inner {
                allowances: Allowances::new(per_hour),
                post_rates: PostRates::new(room_budget::POST_BURST, room_budget::POST_REFILL),
                turns: TurnRuns::new(turns),
                ..Inner::default()
            }),
            next_connection: AtomicU64::new(1),
            last_stamp: AtomicU64::new(0),
            history,
            ingest: IngestLocks::default(),
            persist_budget: PERSIST_BUDGET,
            merge_budget: archive::CHECK_BUDGET,
        }
    }

    /// Shortens how long a history read or write may take, so a test can
    /// exercise the path where the store is slower than the router will wait.
    /// Test-only: no configuration reaches it, and the router always runs on
    /// [`PERSIST_BUDGET`].
    #[cfg(test)]
    pub(crate) fn set_persist_budget(&mut self, budget: Duration) {
        self.persist_budget = budget;
    }

    /// Shortens how long a merge check may take, for the same reason.
    #[cfg(test)]
    pub(crate) fn set_merge_budget(&mut self, budget: Duration) {
        self.merge_budget = budget;
    }

    /// Whether `room` is archived, as the ingest path would read it.
    #[cfg(test)]
    pub(crate) fn is_archived(&self, room: &RoomId) -> bool {
        self.lock().archive.is_archived(room)
    }

    /// No post rate, no token budget and no turn cap. Only for tests about
    /// delivery order or stalled writers, which post far more than any limit
    /// allows. It exists only in test builds, so no configuration can reach it.
    // Must never be `pub`, and must never lose `#[cfg(test)]`: it is a router
    // with every room limit switched off.
    #[cfg(test)]
    pub(crate) fn unmetered() -> Self {
        Self {
            inner: Mutex::new(Inner {
                allowances: Allowances::new(TokensPerHour::UNMETERED),
                post_rates: PostRates::new(u64::MAX, Duration::ZERO),
                turns: TurnRuns::new(TurnCap::UNCAPPED),
                ..Inner::default()
            }),
            next_connection: AtomicU64::new(1),
            last_stamp: AtomicU64::new(0),
            history: Arc::new(crate::history::Fake::new()),
            ingest: IngestLocks::default(),
            persist_budget: PERSIST_BUDGET,
            merge_budget: archive::CHECK_BUDGET,
        }
    }

    /// Registers one authenticated daemon connection, in no rooms yet.
    ///
    /// Returns the queue its room messages arrive on and the handle every
    /// room operation goes through. Dropping the handle takes the connection
    /// out of every room it is in.
    pub fn connect(&self, host: HostId) -> (mpsc::Receiver<Delivery>, Membership<'_>) {
        let (outbox, inbox) = mpsc::channel(ROOM_QUEUE);
        let id = ConnectionId(self.next_connection.fetch_add(1, Ordering::Relaxed));
        self.lock().connections.insert(
            id,
            Connection {
                host: host.clone(),
                outbox,
                rooms: BTreeMap::new(),
                joins: JoinWindow::new(Instant::now()),
                undelivered: 0,
            },
        );
        (
            inbox,
            Membership {
                rooms: self,
                id,
                host,
            },
        )
    }

    /// Every mutation below completes before anything that could panic, and
    /// nothing here panics, so a poisoned lock holds a consistent table.
    /// Recovering it keeps one bad task from taking every room down (rule 39).
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds `id` to `room`. Private, and called only from
    /// [`Membership::try_join`] after [`authorise_join`] granted this room.
    fn admit(&self, id: ConnectionId, room: RoomId) -> Result<RoomId, JoinRefusal> {
        let mut guard = self.lock();
        let Inner {
            connections, rooms, ..
        } = &mut *guard;
        let Some(connection) = connections.get_mut(&id) else {
            return Err(JoinRefusal::Disconnected);
        };
        if connection.rooms.contains_key(&room) {
            return Ok(room);
        }
        if connection.rooms.len() >= MAX_ROOMS_PER_CONNECTION {
            return Err(JoinRefusal::TooManyRooms);
        }
        if rooms
            .get(&room)
            .is_some_and(|existing| existing.members.len() >= MAX_MEMBERS_PER_ROOM)
        {
            return Err(JoinRefusal::RoomFull);
        }
        rooms.entry(room.clone()).or_default().members.insert(id);
        // What delivery relies on: `post` stamps a message and puts it on a
        // connection's queue under `inner`, and only for connections that are
        // members at that moment. So a message already queued here for `room`
        // came from an earlier membership of this connection — ended by its
        // own `Leave` or re-check, both handled in this connection's task
        // before this join — and was stamped no later than the value read
        // here, while every message queued from now on is stamped later.
        connection
            .rooms
            .insert(room.clone(), self.last_stamp.load(Ordering::SeqCst));
        Ok(room)
    }

    /// Takes `id` out of `room`. `false` when it was not a member.
    fn remove(&self, id: ConnectionId, room: &RoomId) -> bool {
        let mut guard = self.lock();
        let Inner {
            connections, rooms, ..
        } = &mut *guard;
        let Some(connection) = connections.get_mut(&id) else {
            return false;
        };
        if connection.rooms.remove(room).is_none() {
            return false;
        }
        leave_room(rooms, id, room);
        true
    }

    /// Every room that has members right now, in key order. A copy, so no lock
    /// is held by whoever goes on to check them.
    fn live_rooms(&self) -> Vec<RoomId> {
        let mut rooms: Vec<RoomId> = self.lock().rooms.keys().cloned().collect();
        rooms.sort();
        rooms
    }

    fn rooms_of(&self, id: ConnectionId) -> Vec<RoomId> {
        self.lock()
            .connections
            .get(&id)
            .map(|connection| connection.rooms.keys().cloned().collect())
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn connection_count(&self) -> usize {
        self.lock().connections.len()
    }

    #[cfg(test)]
    pub(crate) fn room_count(&self) -> usize {
        self.lock().rooms.len()
    }

    #[cfg(test)]
    pub(crate) fn member_count(&self, room: &RoomId) -> usize {
        self.lock()
            .rooms
            .get(room)
            .map_or(0, |room| room.members.len())
    }
}

/// Removes `id` from `room`'s member set and forgets the room once it is empty.
fn leave_room(rooms: &mut HashMap<RoomId, Room>, id: ConnectionId, room: &RoomId) {
    if let Some(entry) = rooms.get_mut(room) {
        entry.members.remove(&id);
        if entry.members.is_empty() {
            rooms.remove(room);
        }
    }
}

/// `room` as its one key: the repo in a `Repo` scope rewritten to the map's
/// spelling. `None` for a room whose project the map does not hold, or whose
/// repo that project does not list — no key exists for such a room, so nothing
/// can be joined, posted to or delivered under one.
fn canonical(projects: &ProjectMap, room: &RoomId) -> Option<RoomId> {
    let listed = projects.repos_of(&room.project)?;
    let scope = match &room.scope {
        Scope::Repo(named) => Scope::Repo(
            listed
                .iter()
                .find(|repo| repo.as_str().eq_ignore_ascii_case(named.as_str()))?
                .clone(),
        ),
        Scope::Ticket(ticket) => Scope::Ticket(ticket.clone()),
        Scope::Project => Scope::Project,
    };
    Some(RoomId::new(room.project.clone(), scope))
}

fn scope_word(scope: &Scope) -> &'static str {
    match scope {
        Scope::Project => "project",
        Scope::Repo(_) => "repo",
        Scope::Ticket(_) => "ticket",
    }
}

/// Why a join was refused. Server-side only: every one of these is the same
/// `ServerFrame::JoinRefused` to the caller (rule 25), and exists so the log
/// can say which (rule 28). `authorise_join` does not expose its own reasons,
/// so they are all `NotAuthorised` here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinRefusal {
    RateLimited,
    NotAuthorised,
    TimedOut,
    TooManyRooms,
    RoomFull,
    Disconnected,
    Busy,
}

impl JoinRefusal {
    fn describe(self) -> &'static str {
        match self {
            Self::RateLimited => "over the join rate limit",
            Self::NotAuthorised => "the join check refused",
            Self::TimedOut => "the join check did not finish in time",
            Self::TooManyRooms => "the connection is already in the most rooms it may hold",
            Self::RoomFull => "the room is full",
            Self::Disconnected => "the connection closed",
            Self::Busy => "another access check on this connection was still running",
        }
    }
}

/// Whether a post may go on to the store, and what it costs to find out.
/// Decided once before the room's ingest lock is taken and again once it is
/// held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seed {
    /// The room already knows where its stored numbering left off.
    Ready,
    /// It does not, and the store should be read.
    Needed,
    /// The store failed recently enough that asking again would just spend
    /// [`PERSIST_BUDGET`] under the room's ingest lock to fail again.
    BackingOff,
    /// The room is archived, so nothing is read or written: the
    /// merge that closed it already happened, and seeding would spend a store
    /// read on a post that is refused whatever it finds.
    Archived,
    /// This connection is not in the room, so nothing is read (rule 44).
    NotAMember,
}

/// Which limit refused a post, in more detail than the wire's `PostRefusal`,
/// for the log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostLimit {
    Rate,
    Allowance,
    AllowanceRooms,
    Turns,
    /// The store refused the message for a reason other than being
    /// unreachable — a sequence collision, or a clock that cannot give it a
    /// retention deadline — so it was delivered to nobody. Not a limit the
    /// poster can do anything about, unlike the four above, which is why it is
    /// logged every time rather than once per run: it is an incident, not a
    /// misbehaving agent. It starts no backoff; the next post takes the next
    /// number.
    Unrecorded,
    /// The message could not be written because the store did not answer in
    /// time or could not be reached, so it was delivered to nobody. Logged,
    /// and starts a [`STORE_BACKOFF`] window for the room.
    Unreachable,
    /// The room's stored numbering could not be read, so no sequence number
    /// could safely be taken. Refusing is the fail-closed direction: starting
    /// from 1 would reuse numbers the table already holds. Logged, and starts
    /// a [`STORE_BACKOFF`] window for the room.
    Unseeded,
    /// Inside a room's [`STORE_BACKOFF`] window. Never logged: the failure
    /// that opened the window was, and says how long it lasts. Refused before
    /// the post rate and before the store, so it spends nothing. A post that
    /// arrives inside the window is refused before the ingest lock and waits
    /// on nothing; one already queued on the lock when the failure happened is
    /// refused as soon as it takes the lock.
    BackingOff,
    /// The ingest lock table is at [`MAX_INGEST_ROOMS`]. Logged every time;
    /// reaching it needs over a thousand connections posting at once.
    IngestFull,
    /// The room is a ticket room whose pull request has merged. Logged once,
    /// when the room is archived, and counted per connection after that: a
    /// log line per refused post would let a retrying agent flood stderr.
    Archived,
}

impl PostLimit {
    fn category(self) -> PostRefusal {
        match self {
            Self::Archived => PostRefusal::Archived,
            Self::Rate => PostRefusal::RateLimited,
            Self::Allowance | Self::AllowanceRooms => PostRefusal::BudgetExhausted,
            Self::Turns => PostRefusal::TurnCapped,
            Self::Unrecorded
            | Self::Unreachable
            | Self::Unseeded
            | Self::BackingOff
            | Self::IngestFull => PostRefusal::NotRecorded,
        }
    }

    /// What the log line says about when this is logged again. A limit the
    /// poster controls is logged once per run, so a retrying agent cannot
    /// flood stderr. A store failure is logged when it happens, because that
    /// is the only signal the table is unreachable, and the backoff it starts
    /// is what bounds how often.
    fn repeats(self) -> String {
        match self {
            Self::Unreachable | Self::Unseeded => format!(
                "Posts to that room are refused for {}s without asking the store, and the next \
                 failure after that is logged again",
                STORE_BACKOFF.as_secs()
            ),
            Self::Unrecorded | Self::IngestFull | Self::BackingOff => {
                "Every occurrence is logged".to_owned()
            }
            Self::Rate | Self::Allowance | Self::AllowanceRooms | Self::Turns | Self::Archived => {
                "Each refusal is answered; this is logged again only after a post is admitted"
                    .to_owned()
            }
        }
    }

    fn describe(self) -> &'static str {
        match self {
            Self::Rate => "the host is over the post rate limit",
            Self::Allowance => "the host has spent its token allowance for that room",
            Self::AllowanceRooms => {
                "the host already holds token allowances in the most rooms it may"
            }
            Self::Turns => {
                "the host and one other have taken the most turns the turn cap allows there \
                 with no third host posting"
            }
            Self::Unrecorded => {
                "the message could not be written to the room's history, so it was delivered \
                 to nobody"
            }
            Self::Unreachable => {
                "the room's history could not be reached in time, so the message was delivered \
                 to nobody"
            }
            Self::Unseeded => {
                "the room's stored sequence could not be read, so no sequence number was taken"
            }
            Self::BackingOff => "the room's history failed recently and is not being asked yet",
            Self::IngestFull => "too many rooms have a post being recorded at once",
            Self::Archived => "the room is archived because its ticket's pull request merged",
        }
    }
}

/// What a post did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Posted {
    /// Handed to `recipients` members' queues, the poster's own included.
    Delivered { seq: RoomSeq, recipients: usize },
    /// The connection is not a member of the room it named, or the room has no
    /// key at all. Nothing was delivered and no sequence number was used.
    NotMember,
    /// The room has used every sequence number. Unreachable in practice, and
    /// refused rather than wrapped, so a sequence number is never reused.
    SequenceExhausted,
    /// A member's post refused by the room's turn cap, its host's post rate,
    /// its host's token allowance for the room, or the room's history store.
    /// Nothing was delivered and the caller answers with
    /// `ServerFrame::PostRefused`. `room` is the router's key for the room.
    ///
    /// The first three take no sequence number. A post refused because it
    /// could not be recorded has taken one, and that number is not reused —
    /// see the module docs.
    Refused { room: RoomId, category: PostRefusal },
}

/// What [`Membership::reserve`] produced: a message with its number and stamp,
/// ready to be written and then delivered.
struct Reserved {
    record: Record,
    stamp: u64,
}

/// One daemon connection's handle on the room registry.
pub struct Membership<'a> {
    rooms: &'a Rooms,
    id: ConnectionId,
    host: HostId,
}

impl Membership<'_> {
    /// Joins `room` if, and only if, `authorise_join` grants it for `token`.
    /// The only path to membership.
    pub async fn join<D: Directory>(
        &self,
        directory: &D,
        projects: &ProjectMap,
        token: &SecretToken,
        room: RoomId,
    ) -> ServerFrame {
        self.join_within(
            directory,
            projects,
            token,
            room,
            JOIN_BUDGET,
            Instant::now(),
        )
        .await
    }

    async fn join_within<D: Directory>(
        &self,
        directory: &D,
        projects: &ProjectMap,
        token: &SecretToken,
        room: RoomId,
        budget: Duration,
        now: Instant,
    ) -> ServerFrame {
        match self
            .try_join(directory, projects, token, &room, budget, now)
            .await
        {
            Ok(canonical) => ServerFrame::Joined { room: canonical },
            Err(refusal) => self.refuse(room, refusal),
        }
    }

    /// Refuses a join that arrived while another access check on this
    /// connection was still running. The same coarse `JoinRefused` as every
    /// other refusal (rule 25), logged with its own reason (rule 28), and it
    /// asks GitHub nothing and does not count against the join rate limit.
    pub fn refuse_busy(&self, room: RoomId) -> ServerFrame {
        self.refuse(room, JoinRefusal::Busy)
    }

    fn refuse(&self, room: RoomId, refusal: JoinRefusal) -> ServerFrame {
        eprintln!(
            "sealmesh-server: refused {}'s join to a {} room in project {}: {}",
            self.host,
            scope_word(&room.scope),
            room.project,
            refusal.describe()
        );
        ServerFrame::JoinRefused { room }
    }

    async fn try_join<D: Directory>(
        &self,
        directory: &D,
        projects: &ProjectMap,
        token: &SecretToken,
        room: &RoomId,
        budget: Duration,
        now: Instant,
    ) -> Result<RoomId, JoinRefusal> {
        let allowed = self
            .rooms
            .lock()
            .connections
            .get_mut(&self.id)
            .map(|connection| connection.joins.allow(now));
        match allowed {
            Some(true) => {}
            Some(false) => return Err(JoinRefusal::RateLimited),
            None => return Err(JoinRefusal::Disconnected),
        }

        match tokio::time::timeout(budget, authorise_join(directory, projects, token, room)).await {
            Ok(Ok(())) => {}
            Ok(Err(_denied)) => return Err(JoinRefusal::NotAuthorised),
            Err(_elapsed) => return Err(JoinRefusal::TimedOut),
        }

        // A grant means the map holds this room, so this cannot miss; if it
        // ever did, refusing is the direction that cannot admit anyone.
        let canonical = canonical(projects, room).ok_or(JoinRefusal::NotAuthorised)?;
        self.rooms.admit(self.id, canonical)
    }

    /// Leaves `room`. `None` when this connection was not in it, which the
    /// caller drops without an answer.
    pub fn leave(&self, projects: &ProjectMap, room: &RoomId) -> Option<ServerFrame> {
        let canonical = canonical(projects, room)?;
        self.rooms
            .remove(self.id, &canonical)
            .then_some(ServerFrame::Left { room: canonical })
    }

    /// Fans a message out to every member of `room`, when this connection is
    /// one of them, none of the room's three limits refuses it — the turn cap,
    /// its host's post rate, and its host's token allowance for the room — and
    /// the message can be written to the room's history.
    ///
    /// Never blocks on a member: one whose queue is full misses it. It does
    /// wait on the history write, for up to [`PERSIST_BUDGET`], because the
    /// message is recorded before anyone receives it — see the module docs.
    ///
    /// For a ticket room this connection is in, it first settles whether the
    /// room's pull request has merged, asking GitHub with `token` — this
    /// connection's own — for up to [`archive::CHECK_BUDGET`] when no verdict
    /// is current. That wait comes before the ingest lock, never under it.
    pub async fn post<D: Directory>(
        &self,
        directory: &D,
        projects: &ProjectMap,
        token: &SecretToken,
        room: &RoomId,
        kind: MessageKind,
        body: Body,
    ) -> Posted {
        self.settle_archive(directory, projects, token, room, Instant::now())
            .await;
        self.post_past_merge_check(projects, room, kind, body).await
    }

    /// Settles whether `room` is archived, when it is a ticket room this
    /// connection holds and no verdict for it is current. Holds no lock while
    /// GitHub is asked, and logs only once it holds none.
    ///
    /// Everything else — a project or repo room, a room with no key, a room
    /// this connection is not in — asks GitHub nothing (rule 44). The ticket
    /// and the repos come from the room's key and the router's own map, never
    /// from anything else the client sent (rule 12).
    pub(crate) async fn settle_archive<D: Directory>(
        &self,
        directory: &D,
        projects: &ProjectMap,
        token: &SecretToken,
        room: &RoomId,
        now: Instant,
    ) {
        let Some(canonical) = canonical(projects, room) else {
            return;
        };
        let Scope::Ticket(ticket) = &canonical.scope else {
            return;
        };
        if !self.holds(&canonical) || self.rooms.lock().archive.is_settled(&canonical, now) {
            return;
        }
        let Some(repos) = projects.repos_of(&canonical.project) else {
            return;
        };
        let verdict =
            archive::check_merged(directory, token, repos, ticket, self.rooms.merge_budget).await;
        let recorded = self.rooms.lock().archive.record(&canonical, verdict, now);
        // Never the ticket, the repos or a branch name (rule 13): the room's
        // project and its kind, as every room log line here.
        match (recorded, verdict) {
            (Recorded::Archived, _) => eprintln!(
                "sealmesh-server: archived a ticket room in project {}: its pull request \
                 merged. It refuses posts from now on; its history stays readable",
                canonical.project
            ),
            (Recorded::Reopened, _) => eprintln!(
                "sealmesh-server: reopened an archived ticket room in project {}: a pull \
                 request for its ticket is open again",
                canonical.project
            ),
            (Recorded::Open, archive::MergeVerdict::Unverifiable) => eprintln!(
                "sealmesh-server: could not verify whether a ticket room in project {} has \
                 merged, checking with {}'s credential; left it open and will ask again in {}s",
                canonical.project,
                self.host,
                archive::UNVERIFIABLE_RETRY.as_secs()
            ),
            _ => {}
        }
    }

    /// [`Membership::post`] for a caller that has already settled the merge
    /// verdict, or a test about everything after it.
    async fn post_past_merge_check(
        &self,
        projects: &ProjectMap,
        room: &RoomId,
        kind: MessageKind,
        body: Body,
    ) -> Posted {
        // A clock before the Unix epoch cannot produce a retention deadline,
        // and a stored message with no expiry is one the seven-day promise
        // does not cover. Refused rather than stored without one.
        let posted_at = match history::now_epoch() {
            Ok(seconds) => seconds,
            Err(_before_epoch) => {
                let Some(canonical) = canonical(projects, room) else {
                    return Posted::NotMember;
                };
                // No ingest guard on this path: it is taken inside
                // `post_holding_ingest`, which a clock this broken never
                // reaches. So logging here holds up nothing.
                let (posted, log) = Self::refusal(canonical, PostLimit::Unrecorded);
                self.log_refusal(log, &posted);
                return posted;
            }
        };
        self.post_within(projects, room, kind, body, Instant::now(), posted_at)
            .await
    }

    /// [`Membership::post`] with both clocks supplied: `now` for the limits,
    /// which are measured against a monotonic instant, and `posted_at` for
    /// what is stored, which has to be wall-clock for a TTL to mean anything.
    #[cfg(test)]
    pub(crate) async fn post_at(
        &self,
        projects: &ProjectMap,
        room: &RoomId,
        kind: MessageKind,
        body: Body,
        now: Instant,
    ) -> Posted {
        self.post_within(projects, room, kind, body, now, test_posted_at())
            .await
    }

    async fn post_within(
        &self,
        projects: &ProjectMap,
        room: &RoomId,
        kind: MessageKind,
        body: Body,
        now: Instant,
        posted_at: u64,
    ) -> Posted {
        let Some(canonical) = canonical(projects, room) else {
            return Posted::NotMember;
        };
        let cost = room_budget::tokens(&body);
        let (posted, log) = self
            .post_holding_ingest(canonical, kind, body, cost, now, posted_at)
            .await;
        // Logged out here, once the guard taken inside `post_holding_ingest`
        // has gone with it. See [`Membership::log_refusal`] for why that
        // matters more than it looks.
        self.log_refusal(log, &posted);
        posted
    }

    /// The part of a post that holds the room's ingest lock: the seed, every
    /// limit, the sequence number, the history write and the fan-out.
    ///
    /// Returns the outcome and the limit its caller should log, and logs
    /// nothing itself — the guard is held until this returns.
    async fn post_holding_ingest(
        &self,
        canonical: RoomId,
        kind: MessageKind,
        body: Body,
        cost: u64,
        now: Instant,
        posted_at: u64,
    ) -> (Posted, Option<PostLimit>) {
        // Decided before the lock, so a post that would be refused without
        // touching the store — to a room this connection is not in, one that
        // is archived, or one backing off after a store failure — is answered
        // at once rather than queued behind a write it has no use for, and a
        // non-member cannot add an entry to the lock table (rule 44).
        if let Some(refused) =
            Self::refused_before_store(&canonical, self.seed_state(&canonical, now))
        {
            return refused;
        }

        // One post at a time per room, from the seed through the write to the
        // fan-out. This is what keeps delivery order and stored order
        // identical; posts to other rooms do not wait on it.
        let Some(lease) = self.rooms.ingest.lease(&canonical) else {
            return Self::refusal(canonical, PostLimit::IngestFull);
        };
        let _ingest = lease.acquire().await;

        // Again under the lock: the post this one queued behind may have
        // seeded the room, failed and started a backoff, or seen it archived,
        // and this connection may have left while it waited.
        let seed = self.seed_state(&canonical, now);
        if let Some(refused) = Self::refused_before_store(&canonical, seed) {
            return refused;
        }
        // A room that has just formed does not know where its numbering left
        // off, so it is seeded from the table before any number is taken.
        if seed == Seed::Needed && !self.seed(&canonical, now).await {
            return Self::refusal(canonical, PostLimit::Unseeded);
        }

        let reserved = match self.reserve(canonical, kind, body, cost, now, posted_at) {
            Ok(reserved) => reserved,
            Err(refusal) => return refusal,
        };

        // Written before anybody receives it. A failure refuses the post and
        // delivers nothing; the number it took is not reused.
        let written = tokio::time::timeout(
            self.rooms.persist_budget,
            self.rooms.history.append(&reserved.record),
        )
        .await;
        match written {
            Ok(Ok(())) => {}
            // The table did not answer: back off, as a failed seed does, so
            // the posts queued behind this one are refused rather than each
            // paying the budget again.
            Ok(Err(HistoryError::Unavailable)) | Err(_) => {
                self.back_off(&reserved.record.room, now);
                return Self::refusal(reserved.record.room.clone(), PostLimit::Unreachable);
            }
            // The table answered and refused, most likely a collision. The
            // next post takes the next number and lands, so nothing backs off.
            Ok(Err(_refused)) => {
                return Self::refusal(reserved.record.room.clone(), PostLimit::Unrecorded);
            }
        }

        let recipients = self.deliver(&reserved, now);
        (
            Posted::Delivered {
                seq: reserved.record.seq,
                recipients,
            },
            None,
        )
    }

    /// A refusal, and the limit whoever is free of the ingest guard should
    /// log for it. Nothing was delivered.
    ///
    /// Deliberately does no logging of its own: most callers are still holding
    /// the guard when it builds one of these.
    fn refusal(room: RoomId, limit: PostLimit) -> (Posted, Option<PostLimit>) {
        (
            Posted::Refused {
                room,
                category: limit.category(),
            },
            Some(limit),
        )
    }

    /// The answer to a post [`Membership::seed_state`] says must not reach
    /// the store, or `None` when it may. None of these is logged: a
    /// non-member and an archived room never were, and a backoff's refusals
    /// are covered by the line that logged the failure starting it.
    fn refused_before_store(room: &RoomId, seed: Seed) -> Option<(Posted, Option<PostLimit>)> {
        let limit = match seed {
            Seed::Ready | Seed::Needed => return None,
            Seed::NotAMember => return Some((Posted::NotMember, None)),
            Seed::Archived => PostLimit::Archived,
            Seed::BackingOff => PostLimit::BackingOff,
        };
        let (posted, _log) = Self::refusal(room.clone(), limit);
        Some((posted, None))
    }

    /// Logged once the ingest guard is gone, never while it is held.
    ///
    /// Every post to the room queues on that guard, so an `eprintln!` that
    /// blocks under it (a full stderr pipe, a stalled log driver) would stall
    /// posting to the room rather than just this connection. The case for
    /// holding that lock across a write rests on everything under it being
    /// bounded, and a write to stderr is not.
    ///
    /// Never the body, never the repo or ticket (rule 13).
    fn log_refusal(&self, limit: Option<PostLimit>, posted: &Posted) {
        let (Some(limit), Posted::Refused { room, .. }) = (limit, posted) else {
            return;
        };
        eprintln!(
            "sealmesh-server: refusing {}'s posts to a {} room in project {}: {}. {}",
            self.host,
            scope_word(&room.scope),
            room.project,
            limit.describe(),
            limit.repeats()
        );
    }

    /// Whether `room` can be posted to yet, and what finding out would cost.
    /// Gated on membership first, so a forged room reads nothing (rule 44),
    /// then on the archive verdict already recorded, so an archived room
    /// reads nothing either.
    fn seed_state(&self, room: &RoomId, now: Instant) -> Seed {
        let guard = self.rooms.lock();
        let Inner {
            connections,
            rooms,
            archive,
            ..
        } = &*guard;
        let member = connections
            .get(&self.id)
            .is_some_and(|connection| connection.rooms.contains_key(room));
        if !member {
            return Seed::NotAMember;
        }
        let Some(entry) = rooms.get(room) else {
            return Seed::NotAMember;
        };
        if archive.is_archived(room) {
            return Seed::Archived;
        }
        // `saturating_duration_since`, so a clock that reads earlier than the
        // recorded failure expires the window rather than underflowing.
        if entry
            .store_failed_at
            .is_some_and(|failed| now.saturating_duration_since(failed) < STORE_BACKOFF)
        {
            return Seed::BackingOff;
        }
        if entry.seeded {
            Seed::Ready
        } else {
            Seed::Needed
        }
    }

    /// Starts a [`STORE_BACKOFF`] window for `room`, when it still exists.
    /// A room that emptied while its write was in flight is gone, and so is
    /// anything there was to back off from.
    fn back_off(&self, room: &RoomId, now: Instant) {
        if let Some(entry) = self.rooms.lock().rooms.get_mut(room) {
            entry.store_failed_at = Some(now);
        }
    }

    /// Reads `room`'s highest persisted sequence number and continues the live
    /// counter from it. `false` when the read could not be made, which refuses
    /// the post rather than numbering from 1 over the top of history, and
    /// starts a [`STORE_BACKOFF`] window so the next posts do not each pay the
    /// budget to fail the same way.
    async fn seed(&self, room: &RoomId, now: Instant) -> bool {
        let highest = tokio::time::timeout(
            self.rooms.persist_budget,
            self.rooms.history.highest_seq(room),
        )
        .await;
        let mut guard = self.rooms.lock();
        let Some(entry) = guard.rooms.get_mut(room) else {
            // The room emptied while the read was in flight, so the seed is
            // moot: there is nothing to record a failure against, and
            // `reserve` reports the membership honestly.
            return true;
        };
        match highest {
            Ok(Ok(highest)) => {
                // `max` never lowers a counter, so applying the seed to
                // whatever room is there now is still correct.
                entry.last_seq = entry.last_seq.max(highest.unwrap_or(0));
                entry.seeded = true;
                entry.store_failed_at = None;
                true
            }
            // Unreachable and timed out are the same fact here: the room's
            // numbering is unknown, so no number may be taken. Bare `_` in
            // both arms because `_name` still binds, and an or-pattern has to
            // bind the same variables in every alternative.
            Ok(Err(_)) | Err(_) => {
                entry.store_failed_at = Some(now);
                false
            }
        }
    }

    /// Runs every limit, then takes a sequence number and a stamp. Holds the
    /// lock and does not deliver: the message is written to history between
    /// this and [`Membership::deliver`].
    ///
    /// `Err` carries the outcome and the limit to log when this refusal starts
    /// a new run of refusals.
    fn reserve(
        &self,
        canonical: RoomId,
        kind: MessageKind,
        body: Body,
        cost: u64,
        now: Instant,
        posted_at: u64,
    ) -> Result<Reserved, (Posted, Option<PostLimit>)> {
        let refuse = |room: RoomId, limit: PostLimit, first: bool| {
            Err((
                Posted::Refused {
                    room,
                    category: limit.category(),
                },
                first.then_some(limit),
            ))
        };

        let mut guard = self.rooms.lock();
        let Inner {
            connections,
            rooms,
            allowances,
            post_rates,
            turns,
            archive,
        } = &mut *guard;
        let Some(connection) = connections.get(&self.id) else {
            return Err((Posted::NotMember, None));
        };
        if !connection.rooms.contains_key(&canonical) {
            return Err((Posted::NotMember, None));
        }
        let Some(entry) = rooms.get_mut(&canonical) else {
            return Err((Posted::NotMember, None));
        };

        // An archived room is closed before any limit runs, so a post to it
        // spends no turn, no rate and no allowance. Only the verdict already
        // recorded is read here: the GitHub call that produced it ran before
        // the ingest lock was taken, in `settle_archive`.
        if archive.is_archived(&canonical) {
            return refuse(canonical, PostLimit::Archived, false);
        }

        // The turn cap runs first, and a post it refuses spends nothing: no
        // rate, no allowance, no stamp and no sequence number. Retrying does
        // not lift a cap, so answering `rate_limited` or `budget_exhausted`
        // first would tell a capped agent to retry for nothing. And the post
        // rate is shared by every room a host posts to, so retries into one
        // capped room must not use it up for the rest. Those retries flood
        // nobody: each refusal goes only to the poster's own socket, and the
        // other participant is told once per capped run.
        if let Turn::Capped { notify } = turns.check(&canonical, &self.host, now) {
            let first = notify.is_some();
            if let Some(other) = notify {
                // Not a post, so it takes no stamp of its own.
                let stamp = self.rooms.last_stamp.load(Ordering::SeqCst);
                for member in &entry.members {
                    let Some(connection) = connections.get_mut(member) else {
                        continue;
                    };
                    // Its own targeting, not left to `deliverable`: only the
                    // other host's connections that hold this room now.
                    if connection.host != other || !connection.rooms.contains_key(&canonical) {
                        continue;
                    }
                    let notice = Delivery {
                        stamp,
                        frame: ServerFrame::TurnCapped {
                            room: canonical.clone(),
                        },
                    };
                    if connection.outbox.try_send(notice).is_err() {
                        connection.undelivered = connection.undelivered.saturating_add(1);
                    }
                }
            }
            return refuse(canonical, PostLimit::Turns, first);
        }

        // Both budget limits run before the sequence number and the fan-out, so a
        // refused post costs the router one lookup and delivers nothing. A
        // post the allowance refuses has still spent its place in the post
        // rate, so a muted connection cannot turn refusals into a flood of
        // their own.
        if let RateSpend::Refused { first } = post_rates.spend(&self.host, now) {
            return refuse(canonical, PostLimit::Rate, first);
        }
        match allowances.spend(&canonical, &self.host, cost, now) {
            Spend::Admitted => {}
            Spend::Refused { why, first } => {
                let limit = match why {
                    Overspent::Exhausted => PostLimit::Allowance,
                    Overspent::TooManyRooms => PostLimit::AllowanceRooms,
                };
                return refuse(canonical, limit, first);
            }
        }

        let Some(next) = entry.last_seq.checked_add(1) else {
            return Err((Posted::SequenceExhausted, None));
        };
        // Advanced while holding `inner`, like the read in `admit`, and refused
        // rather than wrapped at the end of its range, so a stamp never repeats.
        let Ok(previous) =
            self.rooms
                .last_stamp
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |last| {
                    last.checked_add(1)
                })
        else {
            return Err((Posted::SequenceExhausted, None));
        };
        let Some(stamp) = previous.checked_add(1) else {
            return Err((Posted::SequenceExhausted, None));
        };
        entry.last_seq = next;

        Ok(Reserved {
            record: Record {
                room: canonical,
                seq: RoomSeq(next),
                from: self.host.clone(),
                kind,
                body,
                posted_at,
            },
            stamp,
        })
    }

    /// Puts a recorded message on every current member's queue, and counts the
    /// turn it took.
    ///
    /// The turn is counted here and not at reserve time: a turn is something
    /// that happened in the room, and a message nobody received did not
    /// happen. The post rate and the token allowance *are* spent at reserve
    /// time and are not given back, exactly as a post the allowance refuses
    /// has still spent its place in the rate — the router did the work either
    /// way, and refunding would let a host retry a failing write for free.
    fn deliver(&self, reserved: &Reserved, now: Instant) -> usize {
        let mut guard = self.rooms.lock();
        let Inner {
            connections,
            rooms,
            turns,
            ..
        } = &mut *guard;
        // The room can have emptied while the write was in flight — the ingest
        // lock orders posts against each other, not against a `Leave`. Then
        // the message is recorded and reaches nobody live, and a late joiner
        // still reads it from history, which is the point of recording first.
        let Some(entry) = rooms.get(&reserved.record.room) else {
            return 0;
        };
        turns.record(&reserved.record.room, &self.host, now);

        let frame = ServerFrame::Message {
            room: reserved.record.room.clone(),
            seq: reserved.record.seq,
            from: reserved.record.from.clone(),
            kind: reserved.record.kind,
            body: reserved.record.body.clone(),
        };
        let mut recipients = 0usize;
        for member in &entry.members {
            let Some(connection) = connections.get_mut(member) else {
                continue;
            };
            match connection.outbox.try_send(Delivery {
                stamp: reserved.stamp,
                frame: frame.clone(),
            }) {
                Ok(()) => recipients = recipients.saturating_add(1),
                Err(_full_or_closed) => {
                    connection.undelivered = connection.undelivered.saturating_add(1);
                }
            }
        }
        recipients
    }

    /// Re-runs the join check for every room this connection holds, and
    /// leaves each one it no longer passes. Returns a `Left` frame per room
    /// left, for the caller to send.
    pub async fn revalidate<D: Directory>(
        &self,
        directory: &D,
        projects: &ProjectMap,
        token: &SecretToken,
    ) -> Vec<ServerFrame> {
        self.revalidate_within(directory, projects, token, JOIN_BUDGET)
            .await
    }

    async fn revalidate_within<D: Directory>(
        &self,
        directory: &D,
        projects: &ProjectMap,
        token: &SecretToken,
        budget: Duration,
    ) -> Vec<ServerFrame> {
        let now = tokio::time::Instant::now();
        // A deadline that cannot be represented is treated as already passed:
        // every room fails its re-check, the fail-closed side.
        let deadline = now.checked_add(budget).unwrap_or(now);

        let mut lapsed = Vec::new();
        for room in self.rooms.rooms_of(self.id) {
            let kept = matches!(
                tokio::time::timeout_at(
                    deadline,
                    authorise_join(directory, projects, token, &room)
                )
                .await,
                Ok(Ok(()))
            );
            if !kept && self.rooms.remove(self.id, &room) {
                eprintln!(
                    "sealmesh-server: {} no longer passes the join check for a {} room in \
                     project {}; removed it from that room",
                    self.host,
                    scope_word(&room.scope),
                    room.project
                );
                lapsed.push(ServerFrame::Left { room });
            }
        }
        lapsed
    }

    /// Room messages this connection missed because its queue was full.
    pub fn undelivered(&self) -> u64 {
        self.rooms
            .lock()
            .connections
            .get(&self.id)
            .map_or(0, |connection| connection.undelivered)
    }

    /// The next frame from this connection's room queue that it may still
    /// receive. `None` once the queue is closed.
    ///
    /// A message is queued when it is posted, and the connection can leave the
    /// room — or be removed by a re-check — before its socket gets to it.
    /// Sending it anyway would deliver a room's message to a connection that no
    /// longer holds that room, so a message is skipped unless the connection
    /// still holds its room and it was posted after the connection's current
    /// join. Cancel-safe: the only await is `recv`, so dropping this future
    /// loses nothing but frames it had already decided to skip.
    pub async fn next_delivery(&self, inbox: &mut mpsc::Receiver<Delivery>) -> Option<ServerFrame> {
        loop {
            let delivery = inbox.recv().await?;
            if self.deliverable(&delivery) {
                return Some(delivery.frame);
            }
        }
    }

    /// Whether this connection still holds the message's room, and joined it
    /// before the message was posted. Compared by stamp, never by the room's
    /// `seq`, which starts over when the room re-forms.
    ///
    /// A turn-cap notice needs only the room to still be held. It is not a
    /// post and takes no stamp of its own, and the cap it reports outlives a
    /// leave and rejoin, so it is still true after one.
    fn deliverable(&self, delivery: &Delivery) -> bool {
        let (room, posted) = match &delivery.frame {
            ServerFrame::Message { room, .. } => (room, true),
            ServerFrame::TurnCapped { room } => (room, false),
            _ => return true,
        };
        self.rooms
            .lock()
            .connections
            .get(&self.id)
            .and_then(|connection| connection.rooms.get(room))
            .is_some_and(|joined_at| !posted || delivery.stamp > *joined_at)
    }

    /// Whether this connection currently holds the room keyed `key`.
    ///
    /// A `Join` for a room already held is answered `Joined` from this alone,
    /// with no access check: the connection is already a member, and losing
    /// access to the room is the periodic re-check's job. Starting a check
    /// instead would let a `Leave` sent during it cancel a join that could
    /// never have changed anything, while the membership stayed.
    pub fn holds(&self, key: &RoomId) -> bool {
        self.rooms
            .lock()
            .connections
            .get(&self.id)
            .is_some_and(|connection| connection.rooms.contains_key(key))
    }

    /// Answers a `ClientFrame::Backfill`: one frame of `room`'s stored history
    /// after `after`, when this connection holds the room, and a refusal
    /// otherwise.
    ///
    /// Membership is decided before the store is asked anything, so a room
    /// that is someone else's, one with history but no members, and one that
    /// never existed get the same `not_held`, carrying the room as sent, with
    /// no read behind it (rules 25, 44). It is asked again once the read
    /// finishes: a `Leave` or a re-check revocation while the read was in
    /// flight ends the membership the read was allowed under, so its result
    /// is dropped and the same `not_held` sent instead.
    ///
    /// Waits on the store for up to the persist budget, and takes neither the
    /// ingest lock nor any other lock across that wait. The caller polls it as
    /// its own `select!` branch.
    pub async fn backfill(
        &self,
        projects: &ProjectMap,
        room: RoomId,
        after: Option<RoomSeq>,
    ) -> ServerFrame {
        let Some(key) = canonical(projects, &room).filter(|key| self.holds(key)) else {
            return room_reads::refuse(room, BackfillRefusal::NotHeld);
        };
        let read = room_reads::read_reply(
            self.rooms.history.as_ref(),
            &key,
            after,
            self.rooms.persist_budget,
        )
        .await;
        if !self.holds(&key) {
            return room_reads::refuse(room, BackfillRefusal::NotHeld);
        }
        match read {
            Ok(reply) => reply,
            Err(error) => {
                // A category and literals only: `HistoryError` never carries
                // a stored value (rules 14, 26). Logged every time, like an
                // unrecorded post, because it is the signal the table is
                // unreachable.
                eprintln!(
                    "sealmesh-server: could not read {}'s backfill of a {} room in project {}: \
                     {error}",
                    self.host,
                    scope_word(&key.scope),
                    key.project
                );
                room_reads::refuse(room, BackfillRefusal::Unavailable)
            }
        }
    }

    /// Answers a `ClientFrame::Backfill` that arrived while this connection's
    /// previous one was still being read. Reads nothing.
    pub fn refuse_backfill_busy(&self, room: RoomId) -> ServerFrame {
        room_reads::refuse(room, BackfillRefusal::Busy)
    }

    /// Answers a `ClientFrame::ListRooms`: the live rooms `token` — this
    /// connection's own — passes the join check for, bounded and charged to
    /// this connection's host as `room_reads` describes.
    ///
    /// The candidates are copied out of the registry under its short lock and
    /// the lock released before GitHub is asked anything.
    pub async fn list_rooms<D: Directory>(
        &self,
        directory: &D,
        projects: &ProjectMap,
        token: &SecretToken,
        budgets: &CheckBudgets,
    ) -> ServerFrame {
        let candidates = self.rooms.live_rooms();
        let (listed, spend) = room_reads::readable_rooms(
            candidates,
            directory,
            projects,
            token,
            budgets,
            &self.host,
            room_reads::LIST_BUDGET,
        )
        .await;
        // Counts only, never a room: which rooms were left out is exactly
        // what the listing is not allowed to say (rules 13, 25, 28).
        if spend.refused > 0 || spend.timed_out {
            eprintln!(
                "sealmesh-server: {}'s room list left out rooms unchecked: {} access checks \
                 refused over the listing's ceiling of {} or the host's allowance{}",
                self.host,
                spend.refused,
                room_reads::MAX_LIST_CHECKS,
                if spend.timed_out {
                    ", and the listing ran out of time"
                } else {
                    ""
                }
            );
        }
        // `readable_rooms` stops at the cap, so this cannot refuse; if it ever
        // did, an empty list is the direction that reveals nothing.
        ServerFrame::RoomList {
            rooms: RoomList::new(listed).unwrap_or_default(),
        }
    }

    /// `room` as the registry keys it, or `None` for a room that has no key.
    /// Lets the connection loop tell whether a `Leave` names the room of a join
    /// that is still being checked, however the daemon spelled it.
    pub fn room_key(projects: &ProjectMap, room: &RoomId) -> Option<RoomId> {
        canonical(projects, room)
    }

    /// Refuses a join whose room the daemon left while the join was still
    /// being checked. The join is abandoned before it admits; the answer is the
    /// same coarse `JoinRefused` as every refusal (rule 25), logged with its
    /// own reason (rule 28).
    pub fn refuse_cancelled(&self, room: RoomId) -> ServerFrame {
        eprintln!(
            "sealmesh-server: cancelled {}'s join to a {} room in project {}: the daemon left \
             the room before the join check finished",
            self.host,
            scope_word(&room.scope),
            room.project
        );
        ServerFrame::JoinRefused { room }
    }
}

impl Drop for Membership<'_> {
    fn drop(&mut self) {
        let mut guard = self.rooms.lock();
        let Inner {
            connections, rooms, ..
        } = &mut *guard;
        if let Some(connection) = connections.remove(&self.id) {
            for room in connection.rooms.keys() {
                leave_room(rooms, self.id, room);
            }
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

    use std::sync::atomic::AtomicUsize;

    use sealmesh_wire::{ProjectId, RepoSlug, TicketId};

    use super::*;
    use crate::{
        auth::{Denied, RepoAccess},
        history::{Fake, RETENTION, backfill},
        room_budget::{MAX_POST_TOKENS, POST_BURST, POST_REFILL},
    };

    const MAP: &str = r#"[
        {"id":"example-repo","repos":["example-org/example-repo","example-org/example-repo-portal"]},
        {"id":"example-tools","repos":["example-org/example-tools"]}
    ]"#;

    const EXAMPLE_REPO: &str = "example-org/example-repo";
    const PORTAL: &str = "example-org/example-repo-portal";
    const SKILLS: &str = "example-org/example-tools";
    /// In no project in `MAP`.
    const UNMAPPED: &str = "example-org/sealmesh";

    /// Grants exact `(token, repo)` pairs, counts every access check, and can
    /// revoke a grant mid-test.
    struct Grants {
        grants: Mutex<Vec<(String, String)>>,
        calls: AtomicUsize,
    }

    impl Grants {
        fn new(grants: &[(&str, &str)]) -> Self {
            Self {
                grants: Mutex::new(
                    grants
                        .iter()
                        .map(|(t, r)| ((*t).to_owned(), (*r).to_owned()))
                        .collect(),
                ),
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn revoke(&self, token: &str, repo: &str) {
            self.grants
                .lock()
                .unwrap()
                .retain(|(t, r)| !(t == token && r == repo));
        }
    }

    impl Directory for Grants {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn can_read(&self, token: &SecretToken, repo: &RepoSlug) -> RepoAccess {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let granted = self
                .grants
                .lock()
                .unwrap()
                .iter()
                .any(|(t, r)| t == token.expose() && r == repo.as_str());
            if granted {
                RepoAccess::Readable
            } else {
                RepoAccess::NotReadable
            }
        }
    }

    /// GitHub never answering.
    struct Hangs;

    impl Directory for Hangs {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn can_read(&self, _token: &SecretToken, _repo: &RepoSlug) -> RepoAccess {
            std::future::pending::<RepoAccess>().await
        }
    }

    fn map() -> ProjectMap {
        ProjectMap::parse(MAP).unwrap()
    }

    fn host(name: &str) -> HostId {
        HostId::try_from(name).unwrap()
    }

    fn token(value: &str) -> SecretToken {
        SecretToken::new(value.to_owned())
    }

    fn repo_room(project: &str, slug: &str) -> RoomId {
        RoomId::new(
            ProjectId::try_from(project).unwrap(),
            Scope::Repo(RepoSlug::try_from(slug).unwrap()),
        )
    }

    fn ticket_room(project: &str, number: u32) -> RoomId {
        RoomId::new(
            ProjectId::try_from(project).unwrap(),
            Scope::Ticket(TicketId::new("ENG", number).unwrap()),
        )
    }

    fn project_room(project: &str) -> RoomId {
        RoomId::new(ProjectId::try_from(project).unwrap(), Scope::Project)
    }

    fn body(text: &str) -> Body {
        Body::new(text).unwrap()
    }

    /// A registry over a fresh in-memory history store, for the tests that do
    /// not care what ends up in it. No test in this module touches AWS.
    fn rooms() -> Rooms {
        Rooms::new(Arc::new(Fake::new()))
    }

    /// A registry over `store`, for the tests that read it back or make it
    /// fail.
    fn rooms_over(store: Arc<Fake>) -> Rooms {
        Rooms::new(store)
    }

    /// Every frame queued for a connection, filtered or not — what a post
    /// handed to the queue, before `next_delivery` decides what is still
    /// sendable.
    fn drain(inbox: &mut mpsc::Receiver<Delivery>) -> Vec<ServerFrame> {
        let mut frames = Vec::new();
        while let Ok(delivery) = inbox.try_recv() {
            frames.push(delivery.frame);
        }
        frames
    }

    fn bodies(frames: &[ServerFrame]) -> Vec<String> {
        frames
            .iter()
            .filter_map(|frame| match frame {
                ServerFrame::Message { body, .. } => Some(body.as_str().to_owned()),
                _ => None,
            })
            .collect()
    }

    async fn join(
        member: &Membership<'_>,
        directory: &Grants,
        tok: &str,
        room: RoomId,
    ) -> ServerFrame {
        member.join(directory, &map(), &token(tok), room).await
    }

    #[tokio::test]
    async fn a_post_reaches_every_member_and_no_one_else() {
        let rooms = rooms();
        let directory = Grants::new(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_bay", EXAMPLE_REPO),
            ("ghp_cedar", SKILLS),
        ]);
        let room = repo_room("example-repo", EXAMPLE_REPO);

        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        // Connected, and a member of a different room.
        let (mut cedar_inbox, cedar) = rooms.connect(host("cedar"));
        // Connected, and refused this room.
        let (mut dawn_inbox, dawn) = rooms.connect(host("dawn"));

        let joined = ServerFrame::Joined { room: room.clone() };
        assert_eq!(
            join(&atlas, &directory, "ghp_atlas", room.clone()).await,
            joined
        );
        assert_eq!(
            join(&bay, &directory, "ghp_bay", room.clone()).await,
            joined
        );
        assert!(matches!(
            join(
                &cedar,
                &directory,
                "ghp_cedar",
                ticket_room("example-tools", 1)
            )
            .await,
            ServerFrame::Joined { .. }
        ));
        assert_eq!(
            join(&dawn, &directory, "ghp_dawn", room.clone()).await,
            ServerFrame::JoinRefused { room: room.clone() }
        );

        let posted = atlas
            .post_past_merge_check(&map(), &room, MessageKind::Claim, body("taking the portal"))
            .await;
        assert_eq!(
            posted,
            Posted::Delivered {
                seq: RoomSeq(1),
                recipients: 2
            }
        );

        let expected = ServerFrame::Message {
            room: room.clone(),
            seq: RoomSeq(1),
            from: host("atlas"),
            kind: MessageKind::Claim,
            body: body("taking the portal"),
        };
        assert_eq!(drain(&mut atlas_inbox), vec![expected.clone()]);
        assert_eq!(drain(&mut bay_inbox), vec![expected]);
        assert!(
            drain(&mut cedar_inbox).is_empty(),
            "a member of another room received it"
        );
        assert!(
            drain(&mut dawn_inbox).is_empty(),
            "a refused caller received it"
        );
    }

    #[tokio::test]
    async fn a_daemon_whose_repo_is_in_no_project_cannot_join_or_receive_any_room() {
        // Operator decision on XYZ-59: a repo in no project gets no room, and
        // there is no per-repo fallback room keyed by its name.
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_uplink", UNMAPPED)]);
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        let (mut uplink_inbox, uplink) = rooms.connect(host("uplink"));
        join(&atlas, &directory, "ghp_atlas", example_repo.clone()).await;

        // Every way a room for that repo could be named — under a real
        // project, under a project named after it, and every scope of that
        // project.
        for attempt in [
            repo_room("example-repo", UNMAPPED),
            repo_room("sealmesh", UNMAPPED),
            project_room("sealmesh"),
            ticket_room("sealmesh", 59),
        ] {
            assert_eq!(
                join(&uplink, &directory, "ghp_uplink", attempt.clone()).await,
                ServerFrame::JoinRefused {
                    room: attempt.clone()
                },
                "{attempt:?}"
            );
            assert!(
                canonical(&map(), &attempt).is_none(),
                "a room key was built for {attempt:?}"
            );
            assert_eq!(
                uplink
                    .post_past_merge_check(&map(), &attempt, MessageKind::Claim, body("hello"))
                    .await,
                Posted::NotMember
            );
            assert_eq!(uplink.leave(&map(), &attempt), None);
        }
        assert_eq!(
            rooms.room_count(),
            1,
            "only atlas's room exists; nothing was keyed by the unmapped repo"
        );

        atlas
            .post_past_merge_check(
                &map(),
                &example_repo,
                MessageKind::Question,
                body("anyone?"),
            )
            .await;
        assert_eq!(bodies(&drain(&mut atlas_inbox)), vec!["anyone?"]);
        assert!(drain(&mut uplink_inbox).is_empty());
        assert_eq!(
            directory.calls(),
            1,
            "only atlas's join reached GitHub; every unmapped attempt was refused by the map"
        );
    }

    #[tokio::test]
    async fn every_join_runs_the_check_even_into_a_room_that_already_exists() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (_b, bay) = rooms.connect(host("bay"));
        let (_m, magenta) = rooms.connect(host("magenta"));

        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        assert_eq!(directory.calls(), 1);

        // The room exists and has a member; that is not a reason to skip the
        // check for the next caller.
        join(&bay, &directory, "ghp_bay", room.clone()).await;
        assert_eq!(directory.calls(), 2);
        assert_eq!(
            join(&magenta, &directory, "ghp_magenta", room.clone()).await,
            ServerFrame::JoinRefused { room: room.clone() }
        );
        assert_eq!(directory.calls(), 3);

        // Nor is already being a member.
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        assert_eq!(directory.calls(), 4);
        assert_eq!(rooms.member_count(&room), 2);
    }

    #[tokio::test]
    async fn one_repo_spelled_two_ways_is_one_room_under_the_maps_spelling() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let mapped = repo_room("example-repo", EXAMPLE_REPO);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));

        assert_eq!(
            join(
                &atlas,
                &directory,
                "ghp_atlas",
                repo_room("example-repo", "Example-org/Example-repo")
            )
            .await,
            ServerFrame::Joined {
                room: mapped.clone()
            }
        );
        assert_eq!(
            join(&bay, &directory, "ghp_bay", mapped.clone()).await,
            ServerFrame::Joined {
                room: mapped.clone()
            }
        );
        assert_eq!(rooms.room_count(), 1);

        let posted = bay
            .post_past_merge_check(
                &map(),
                &repo_room("example-repo", "example-org/EXAMPLE-REPO"),
                MessageKind::Ack,
                body("ok"),
            )
            .await;
        assert!(matches!(posted, Posted::Delivered { recipients: 2, .. }));
        let received = drain(&mut atlas_inbox);
        assert!(
            matches!(received.as_slice(), [ServerFrame::Message { room, .. }] if *room == mapped),
            "{received:?}"
        );
        assert_eq!(drain(&mut bay_inbox).len(), 1);

        // A refusal names the room as the caller spelled it, not the map's
        // spelling, so it reveals nothing about the map.
        let (_c, cedar) = rooms.connect(host("cedar"));
        let asked = repo_room("example-repo", "Example-org/Example-repo");
        assert_eq!(
            join(&cedar, &directory, "ghp_cedar", asked.clone()).await,
            ServerFrame::JoinRefused { room: asked }
        );
    }

    #[tokio::test]
    async fn a_post_to_a_room_this_connection_has_not_joined_delivers_nothing() {
        let rooms = rooms();
        let directory = Grants::new(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_elm", EXAMPLE_REPO),
            ("ghp_elm", PORTAL),
        ]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        let (mut elm_inbox, elm) = rooms.connect(host("elm"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        // A member of another room in the same project, who could join this
        // one but has not.
        join(&elm, &directory, "ghp_elm", ticket_room("example-repo", 7)).await;

        for forged in [
            room.clone(),
            project_room("example-repo"),
            project_room("ghost"),
        ] {
            assert_eq!(
                elm.post_past_merge_check(&map(), &forged, MessageKind::Claim, body("forged"))
                    .await,
                Posted::NotMember,
                "{forged:?}"
            );
        }
        assert!(drain(&mut atlas_inbox).is_empty());
        assert!(drain(&mut elm_inbox).is_empty());

        // No sequence number was spent on the dropped posts.
        assert!(matches!(
            atlas
                .post_past_merge_check(&map(), &room, MessageKind::Claim, body("real"))
                .await,
            Posted::Delivered {
                seq: RoomSeq(1),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn a_member_that_stops_reading_misses_messages_without_holding_up_anyone_else() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        join(&bay, &directory, "ghp_bay", room.clone()).await;

        let over = 3;
        let start = Instant::now();
        for i in 0..ROOM_QUEUE + over {
            // bay never reads, atlas reads every message as it arrives. One
            // post per refill interval, so only bay's queue can refuse.
            let at = start + POST_REFILL * u32::try_from(i).unwrap();
            let posted = atlas
                .post_at(
                    &map(),
                    &room,
                    MessageKind::Claim,
                    body(&format!("m{i}")),
                    at,
                )
                .await;
            let recipients = if i < ROOM_QUEUE { 2 } else { 1 };
            assert!(
                matches!(posted, Posted::Delivered { recipients: r, .. } if r == recipients),
                "post {i}: {posted:?}"
            );
            assert_eq!(drain(&mut atlas_inbox).len(), 1, "post {i}");
        }

        assert_eq!(bay.undelivered(), u64::try_from(over).unwrap());
        assert_eq!(atlas.undelivered(), 0);
        assert_eq!(
            drain(&mut bay_inbox).len(),
            ROOM_QUEUE,
            "a stalled member holds at most its queue"
        );
    }

    #[tokio::test]
    async fn a_connection_cannot_hold_more_rooms_than_the_cap() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_atlas", PORTAL)]);
        let (_inbox, atlas) = rooms.connect(host("atlas"));
        let started = Instant::now();
        let projects = map();
        let atlas_token = token("ghp_atlas");

        // Each join in its own rate window, so only the room cap can refuse.
        let mut window = 0u32;
        let mut next_join = |room: RoomId| {
            window += 1;
            atlas.join_within(
                &directory,
                &projects,
                &atlas_token,
                room,
                JOIN_BUDGET,
                started + JOIN_WINDOW * window,
            )
        };
        for n in 1..=u32::try_from(MAX_ROOMS_PER_CONNECTION).unwrap() {
            assert!(matches!(
                next_join(ticket_room("example-repo", n)).await,
                ServerFrame::Joined { .. }
            ));
        }
        let one_more = ticket_room("example-repo", 999);
        assert_eq!(
            next_join(one_more.clone()).await,
            ServerFrame::JoinRefused { room: one_more }
        );
        assert_eq!(rooms.room_count(), MAX_ROOMS_PER_CONNECTION);
    }

    #[tokio::test]
    async fn a_room_cannot_hold_more_members_than_the_cap() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_shared", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let connections: Vec<_> = (0..=MAX_MEMBERS_PER_ROOM)
            .map(|i| rooms.connect(host(&format!("h{i}"))))
            .collect();

        for (i, (_inbox, member)) in connections.iter().enumerate() {
            let reply = join(member, &directory, "ghp_shared", room.clone()).await;
            if i < MAX_MEMBERS_PER_ROOM {
                assert!(matches!(reply, ServerFrame::Joined { .. }), "member {i}");
            } else {
                assert_eq!(reply, ServerFrame::JoinRefused { room: room.clone() });
            }
        }
        assert_eq!(rooms.member_count(&room), MAX_MEMBERS_PER_ROOM);
    }

    #[tokio::test]
    async fn joins_past_the_rate_limit_are_refused_before_github_is_asked() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_inbox, atlas) = rooms.connect(host("atlas"));
        let now = Instant::now();
        let limit = usize::try_from(MAX_JOINS_PER_WINDOW).unwrap();
        let projects = map();
        let atlas_token = token("ghp_atlas");

        let attempt = |at: Instant| {
            atlas.join_within(
                &directory,
                &projects,
                &atlas_token,
                room.clone(),
                JOIN_BUDGET,
                at,
            )
        };
        for _ in 0..limit {
            assert!(matches!(attempt(now).await, ServerFrame::Joined { .. }));
        }
        assert_eq!(directory.calls(), limit);

        assert_eq!(
            attempt(now).await,
            ServerFrame::JoinRefused { room: room.clone() }
        );
        assert_eq!(directory.calls(), limit, "a rate-limited join asked GitHub");

        assert!(matches!(
            attempt(now + JOIN_WINDOW).await,
            ServerFrame::Joined { .. }
        ));
    }

    #[test]
    fn the_join_window_resets_only_once_it_has_passed() {
        let start = Instant::now();
        let mut window = JoinWindow::new(start);
        for _ in 0..MAX_JOINS_PER_WINDOW {
            assert!(window.allow(start));
        }
        assert!(!window.allow(start));
        assert!(!window.allow(start + JOIN_WINDOW - Duration::from_millis(1)));
        assert!(window.allow(start + JOIN_WINDOW));
        // A clock that reads earlier than the window start does not reset it.
        let mut early = JoinWindow::new(start + JOIN_WINDOW);
        for _ in 0..MAX_JOINS_PER_WINDOW {
            assert!(early.allow(start));
        }
        assert!(!early.allow(start));
    }

    #[tokio::test]
    async fn a_join_the_directory_cannot_answer_in_time_is_refused() {
        let rooms = rooms();
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_inbox, atlas) = rooms.connect(host("atlas"));

        let reply = tokio::time::timeout(
            Duration::from_secs(5),
            atlas.join_within(
                &Hangs,
                &map(),
                &token("ghp_atlas"),
                room.clone(),
                Duration::from_millis(50),
                Instant::now(),
            ),
        )
        .await
        .expect("the join budget did not bound a directory that never answers");

        assert_eq!(reply, ServerFrame::JoinRefused { room });
        assert_eq!(rooms.room_count(), 0);
    }

    #[tokio::test]
    async fn a_disconnect_leaves_every_room_and_an_empty_room_is_forgotten() {
        let rooms = rooms();
        let directory = Grants::new(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_atlas", PORTAL),
            ("ghp_bay", EXAMPLE_REPO),
        ]);
        let repo = repo_room("example-repo", EXAMPLE_REPO);
        let ticket = ticket_room("example-repo", 557);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", repo.clone()).await;
        join(&atlas, &directory, "ghp_atlas", ticket.clone()).await;
        join(&bay, &directory, "ghp_bay", repo.clone()).await;
        assert_eq!(rooms.room_count(), 2);

        drop(atlas);
        assert_eq!(rooms.connection_count(), 1);
        assert_eq!(rooms.room_count(), 1, "atlas's ticket room had no one left");
        assert_eq!(rooms.member_count(&repo), 1);
        assert!(matches!(
            bay.post_past_merge_check(&map(), &repo, MessageKind::Ack, body("alone"))
                .await,
            Posted::Delivered { recipients: 1, .. }
        ));
        assert_eq!(drain(&mut bay_inbox).len(), 1);

        drop(bay);
        assert_eq!(rooms.connection_count(), 0);
        assert_eq!(rooms.room_count(), 0);
    }

    #[tokio::test]
    async fn leaving_stops_delivery_and_leaving_a_room_never_joined_does_nothing() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        join(&bay, &directory, "ghp_bay", room.clone()).await;

        assert_eq!(
            bay.leave(
                &map(),
                &repo_room("example-repo", "Example-org/example-repo")
            ),
            Some(ServerFrame::Left { room: room.clone() })
        );
        assert_eq!(bay.leave(&map(), &room), None, "left twice");
        assert_eq!(bay.leave(&map(), &project_room("ghost")), None);

        assert!(matches!(
            atlas
                .post_past_merge_check(&map(), &room, MessageKind::Claim, body("after"))
                .await,
            Posted::Delivered { recipients: 1, .. }
        ));
        assert!(drain(&mut bay_inbox).is_empty());
        assert_eq!(
            bay.post_past_merge_check(&map(), &room, MessageKind::Claim, body("still here?"))
                .await,
            Posted::NotMember
        );
    }

    #[tokio::test]
    async fn sequence_numbers_are_the_routers_and_count_per_room() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_atlas", SKILLS)]);
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let skills = repo_room("example-tools", SKILLS);
        let (_inbox, atlas) = rooms.connect(host("atlas"));
        join(&atlas, &directory, "ghp_atlas", example_repo.clone()).await;
        join(&atlas, &directory, "ghp_atlas", skills.clone()).await;

        let seq = |posted: Posted| match posted {
            Posted::Delivered { seq, .. } => seq,
            other => panic!("{other:?}"),
        };
        // The map is bound rather than built per call: a posting future borrows
        // it, so a temporary would not outlive the await.
        let projects = map();
        let post = async |room: &RoomId| {
            atlas
                .post_past_merge_check(&projects, room, MessageKind::Claim, body("x"))
                .await
        };
        assert_eq!(seq(post(&example_repo).await), RoomSeq(1));
        assert_eq!(seq(post(&example_repo).await), RoomSeq(2));
        assert_eq!(seq(post(&skills).await), RoomSeq(1));
        assert_eq!(seq(post(&example_repo).await), RoomSeq(3));
    }

    #[tokio::test]
    async fn a_member_who_loses_access_is_removed_at_revalidation_and_one_who_keeps_it_stays() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        join(&bay, &directory, "ghp_bay", room.clone()).await;

        directory.revoke("ghp_bay", EXAMPLE_REPO);

        assert!(
            atlas
                .revalidate(&directory, &map(), &token("ghp_atlas"))
                .await
                .is_empty()
        );
        assert_eq!(
            bay.revalidate(&directory, &map(), &token("ghp_bay")).await,
            vec![ServerFrame::Left { room: room.clone() }]
        );

        assert!(matches!(
            atlas
                .post_past_merge_check(&map(), &room, MessageKind::Claim, body("after revoke"))
                .await,
            Posted::Delivered { recipients: 1, .. }
        ));
        assert!(drain(&mut bay_inbox).is_empty());
        assert_eq!(
            bay.post_past_merge_check(&map(), &room, MessageKind::Claim, body("x"))
                .await,
            Posted::NotMember
        );
    }

    /// Every frame `next_delivery` would still hand this connection's socket,
    /// stopping once nothing more is ready.
    async fn still_sendable(
        member: &Membership<'_>,
        inbox: &mut mpsc::Receiver<Delivery>,
    ) -> Vec<ServerFrame> {
        let mut frames = Vec::new();
        while let Ok(Some(frame)) =
            tokio::time::timeout(Duration::from_millis(20), member.next_delivery(inbox)).await
        {
            frames.push(frame);
        }
        frames
    }

    #[tokio::test]
    async fn a_message_queued_before_a_voluntary_leave_is_not_sent_after_it() {
        let rooms = rooms();
        let directory = Grants::new(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_atlas", SKILLS),
            ("ghp_bay", EXAMPLE_REPO),
            ("ghp_bay", SKILLS),
        ]);
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let skills = repo_room("example-tools", SKILLS);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        for room in [&example_repo, &skills] {
            join(&atlas, &directory, "ghp_atlas", room.clone()).await;
            join(&bay, &directory, "ghp_bay", room.clone()).await;
        }

        // Queued for bay and not yet written to his socket.
        for text in ["one", "two", "three"] {
            atlas
                .post_past_merge_check(&map(), &example_repo, MessageKind::Claim, body(text))
                .await;
        }
        atlas
            .post_past_merge_check(
                &map(),
                &skills,
                MessageKind::Claim,
                body("still a member here"),
            )
            .await;

        assert_eq!(
            bay.leave(&map(), &example_repo),
            Some(ServerFrame::Left {
                room: example_repo.clone()
            })
        );
        assert_eq!(
            bodies(&still_sendable(&bay, &mut bay_inbox).await),
            vec!["still a member here"],
            "a message for a room bay left went out after his Left"
        );
    }

    #[tokio::test]
    async fn a_message_queued_before_a_revocation_is_not_sent_after_the_left() {
        let rooms = rooms();
        let directory = Grants::new(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_atlas", SKILLS),
            ("ghp_bay", EXAMPLE_REPO),
            ("ghp_bay", SKILLS),
        ]);
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let skills = repo_room("example-tools", SKILLS);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        for room in [&example_repo, &skills] {
            join(&atlas, &directory, "ghp_atlas", room.clone()).await;
            join(&bay, &directory, "ghp_bay", room.clone()).await;
        }

        for text in ["one", "two", "three"] {
            atlas
                .post_past_merge_check(&map(), &example_repo, MessageKind::Claim, body(text))
                .await;
        }
        atlas
            .post_past_merge_check(
                &map(),
                &skills,
                MessageKind::Claim,
                body("still a member here"),
            )
            .await;

        directory.revoke("ghp_bay", EXAMPLE_REPO);
        assert_eq!(
            bay.revalidate(&directory, &map(), &token("ghp_bay")).await,
            vec![ServerFrame::Left {
                room: example_repo.clone()
            }]
        );
        assert_eq!(
            bodies(&still_sendable(&bay, &mut bay_inbox).await),
            vec!["still a member here"],
            "a message for a room bay lost access to went out after his Left"
        );
    }

    #[tokio::test]
    async fn a_message_queued_before_a_leave_and_rejoin_is_not_sent_after_the_rejoin() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", example_repo.clone()).await;
        join(&bay, &directory, "ghp_bay", example_repo.clone()).await;

        atlas
            .post_past_merge_check(&map(), &example_repo, MessageKind::Claim, body("before"))
            .await;
        bay.leave(&map(), &example_repo);
        join(&bay, &directory, "ghp_bay", example_repo.clone()).await;
        atlas
            .post_past_merge_check(&map(), &example_repo, MessageKind::Claim, body("after"))
            .await;

        assert_eq!(
            bodies(&still_sendable(&bay, &mut bay_inbox).await),
            vec!["after"]
        );
    }

    #[tokio::test]
    async fn a_message_queued_before_a_solo_room_re_forms_is_not_sent_after_the_rejoin() {
        // bay is alone, so his leave empties the room and the rejoin creates
        // it afresh — the case where a per-room counter starts over.
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_bay", EXAMPLE_REPO)]);
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        join(&bay, &directory, "ghp_bay", example_repo.clone()).await;

        // Queued to bay's own room queue and not yet written.
        bay.post_past_merge_check(&map(), &example_repo, MessageKind::Claim, body("old1"))
            .await;
        bay.post_past_merge_check(&map(), &example_repo, MessageKind::Claim, body("old2"))
            .await;
        assert!(bay.leave(&map(), &example_repo).is_some());
        assert_eq!(rooms.room_count(), 0, "the room was not forgotten");

        join(&bay, &directory, "ghp_bay", example_repo.clone()).await;
        bay.post_past_merge_check(&map(), &example_repo, MessageKind::Claim, body("fresh"))
            .await;

        assert_eq!(
            bodies(&still_sendable(&bay, &mut bay_inbox).await),
            vec!["fresh"],
            "a message from the room's previous life was sent after the rejoin"
        );
    }

    #[tokio::test]
    async fn a_message_queued_before_a_shared_room_re_forms_is_not_sent_after_the_rejoin() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let (_atlas_inbox, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", example_repo.clone()).await;
        join(&bay, &directory, "ghp_bay", example_repo.clone()).await;

        for text in ["old1", "old2", "old3"] {
            atlas
                .post_past_merge_check(&map(), &example_repo, MessageKind::Claim, body(text))
                .await;
        }
        assert!(bay.leave(&map(), &example_repo).is_some());
        assert!(atlas.leave(&map(), &example_repo).is_some());
        assert_eq!(rooms.room_count(), 0, "the room was not forgotten");

        join(&bay, &directory, "ghp_bay", example_repo.clone()).await;
        assert!(
            still_sendable(&bay, &mut bay_inbox).await.is_empty(),
            "a message from the room's previous life was sent after the rejoin"
        );

        bay.post_past_merge_check(&map(), &example_repo, MessageKind::Claim, body("fresh"))
            .await;
        assert_eq!(
            bodies(&still_sendable(&bay, &mut bay_inbox).await),
            vec!["fresh"]
        );
    }

    #[tokio::test]
    async fn a_revalidation_that_cannot_finish_in_time_removes_the_member() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_inbox, atlas) = rooms.connect(host("atlas"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;

        let lapsed = tokio::time::timeout(
            Duration::from_secs(5),
            atlas.revalidate_within(
                &Hangs,
                &map(),
                &token("ghp_atlas"),
                Duration::from_millis(50),
            ),
        )
        .await
        .expect("the re-check budget did not bound a directory that never answers");

        assert_eq!(lapsed, vec![ServerFrame::Left { room: room.clone() }]);
        assert_eq!(rooms.member_count(&room), 0);
    }

    /// The most expensive body there is: every character four UTF-8 bytes.
    fn widest() -> Body {
        Body::new("😀".repeat(sealmesh_chat_wire::MAX_BODY_CHARS)).unwrap()
    }

    fn refused(room: &RoomId, category: PostRefusal) -> Posted {
        Posted::Refused {
            room: room.clone(),
            category,
        }
    }

    /// How many widest posts the default allowance covers from full.
    fn widest_that_fit() -> u64 {
        TokensPerHour::DEFAULT.get() / MAX_POST_TOKENS
    }

    #[tokio::test]
    async fn a_flooding_member_cannot_stop_another_members_post_from_arriving() {
        // The XYZ-59 review finding: one member posting fast enough to fill
        // every other member's queue, so everyone else's messages drop.
        let rooms = rooms();
        let directory = Grants::new(&[
            ("ghp_magenta", EXAMPLE_REPO),
            ("ghp_bay", EXAMPLE_REPO),
            ("ghp_cedar", EXAMPLE_REPO),
        ]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_m, magenta) = rooms.connect(host("magenta"));
        // bay does not read until the end: the member a flood would fill.
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        let (_c, cedar) = rooms.connect(host("cedar"));
        for (member, tok) in [
            (&magenta, "ghp_magenta"),
            (&bay, "ghp_bay"),
            (&cedar, "ghp_cedar"),
        ] {
            join(member, &directory, tok, room.clone()).await;
        }

        // Small bodies, so the token budget never comes into it.
        let now = Instant::now();
        let mut admitted = 0u64;
        for i in 0..ROOM_QUEUE + 8 {
            let posted = magenta
                .post_at(
                    &map(),
                    &room,
                    MessageKind::Claim,
                    body(&format!("flood {i}")),
                    now,
                )
                .await;
            match posted {
                Posted::Delivered { .. } => admitted += 1,
                other => assert_eq!(other, refused(&room, PostRefusal::RateLimited), "post {i}"),
            }
        }
        assert_eq!(admitted, POST_BURST);

        let posted = cedar
            .post_at(
                &map(),
                &room,
                MessageKind::Handoff,
                body("cedar's handoff"),
                now,
            )
            .await;
        assert!(
            matches!(posted, Posted::Delivered { recipients: 3, .. }),
            "the flood kept cedar's post from a member: {posted:?}"
        );
        let received = bodies(&drain(&mut bay_inbox));
        assert_eq!(received.len(), usize::try_from(POST_BURST + 1).unwrap());
        assert_eq!(received.last().map(String::as_str), Some("cedar's handoff"));
    }

    #[tokio::test]
    async fn a_post_over_the_rate_limit_is_refused_uses_no_sequence_number_and_recovers() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        join(&bay, &directory, "ghp_bay", room.clone()).await;

        let start = Instant::now();
        let projects = map();
        let post = async |text: &str, at: Instant| {
            atlas
                .post_at(&projects, &room, MessageKind::Claim, body(text), at)
                .await
        };
        for n in 1..=POST_BURST {
            assert_eq!(
                post("within", start).await,
                Posted::Delivered {
                    seq: RoomSeq(n),
                    recipients: 2
                }
            );
        }
        // Spelled differently from the join: the refusal names the router's
        // key for the room.
        assert_eq!(
            atlas
                .post_at(
                    &map(),
                    &repo_room("example-repo", "Example-org/Example-repo"),
                    MessageKind::Claim,
                    body("over"),
                    start
                )
                .await,
            refused(&room, PostRefusal::RateLimited)
        );
        assert_eq!(
            post("over", start + POST_REFILL - Duration::from_millis(1)).await,
            refused(&room, PostRefusal::RateLimited)
        );
        assert_eq!(
            drain(&mut bay_inbox).len(),
            usize::try_from(POST_BURST).unwrap(),
            "a refused post was delivered"
        );

        assert_eq!(
            post("back", start + POST_REFILL).await,
            Posted::Delivered {
                seq: RoomSeq(POST_BURST + 1),
                recipients: 2
            },
            "a refused post used a sequence number, or the rate never recovered"
        );
    }

    #[tokio::test]
    async fn a_host_that_spends_its_allowance_is_refused_in_that_room_only_and_recovers() {
        let rooms = rooms();
        let directory = Grants::new(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_atlas", SKILLS),
            ("ghp_bay", EXAMPLE_REPO),
        ]);
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let skills = repo_room("example-tools", SKILLS);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", example_repo.clone()).await;
        join(&atlas, &directory, "ghp_atlas", skills.clone()).await;
        join(&bay, &directory, "ghp_bay", example_repo.clone()).await;

        let start = Instant::now();
        let fits = widest_that_fit();
        assert!(fits < POST_BURST, "the post rate would refuse first");
        // The room spelled as the map does and as a daemon might: one
        // allowance, not two.
        let shouted = repo_room("example-repo", "Example-org/Example-repo");
        for n in 1..=fits {
            let spelling = if n % 2 == 0 { &shouted } else { &example_repo };
            assert_eq!(
                atlas
                    .post_at(&map(), spelling, MessageKind::Claim, widest(), start)
                    .await,
                Posted::Delivered {
                    seq: RoomSeq(n),
                    recipients: 2
                }
            );
        }
        assert_eq!(
            atlas
                .post_at(&map(), &shouted, MessageKind::Claim, widest(), start)
                .await,
            refused(&example_repo, PostRefusal::BudgetExhausted),
            "a different spelling of the room drew on a separate allowance"
        );
        assert_eq!(
            drain(&mut bay_inbox).len(),
            usize::try_from(fits).unwrap(),
            "a refused post was delivered"
        );

        // Another host, same room: atlas's spending is not bay's.
        assert_eq!(
            bay.post_at(&map(), &example_repo, MessageKind::Claim, widest(), start)
                .await,
            Posted::Delivered {
                seq: RoomSeq(fits + 1),
                recipients: 2
            }
        );
        // Same host, another room: a loop in one room does not mute the rest.
        assert!(matches!(
            atlas
                .post_at(&map(), &skills, MessageKind::Claim, widest(), start)
                .await,
            Posted::Delivered {
                seq: RoomSeq(1),
                ..
            }
        ));

        // The allowance refills one token per hour/allowance. atlas is short
        // by the difference between one widest post and what she has left.
        let left = TokensPerHour::DEFAULT.get() - fits * MAX_POST_TOKENS;
        let short = u32::try_from(MAX_POST_TOKENS - left).unwrap();
        let per_token =
            Duration::from_secs(3600) / u32::try_from(TokensPerHour::DEFAULT.get()).unwrap();
        let ready = start + per_token * short;
        assert_eq!(
            atlas
                .post_at(
                    &map(),
                    &example_repo,
                    MessageKind::Claim,
                    widest(),
                    ready - Duration::from_millis(1)
                )
                .await,
            refused(&example_repo, PostRefusal::BudgetExhausted)
        );
        assert_eq!(
            atlas
                .post_at(&map(), &example_repo, MessageKind::Claim, widest(), ready)
                .await,
            Posted::Delivered {
                seq: RoomSeq(fits + 2),
                recipients: 2
            }
        );
    }

    #[tokio::test]
    async fn neither_a_reconnect_nor_a_room_re_forming_resets_a_hosts_allowance() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO)]);
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let start = Instant::now();
        {
            let (_inbox, atlas) = rooms.connect(host("atlas"));
            join(&atlas, &directory, "ghp_atlas", example_repo.clone()).await;
            for _ in 0..widest_that_fit() {
                assert!(matches!(
                    atlas
                        .post_at(&map(), &example_repo, MessageKind::Claim, widest(), start)
                        .await,
                    Posted::Delivered { .. }
                ));
            }
            assert!(atlas.leave(&map(), &example_repo).is_some());
            assert_eq!(rooms.room_count(), 0, "the room did not empty");
        }
        assert_eq!(rooms.connection_count(), 0, "the connection did not close");

        let (_inbox, atlas) = rooms.connect(host("atlas"));
        join(&atlas, &directory, "ghp_atlas", example_repo.clone()).await;
        assert_eq!(
            atlas
                .post_at(&map(), &example_repo, MessageKind::Claim, widest(), start)
                .await,
            refused(&example_repo, PostRefusal::BudgetExhausted)
        );
    }

    #[tokio::test]
    async fn a_post_to_a_room_not_joined_spends_neither_the_rate_nor_the_allowance() {
        let rooms = rooms();
        let directory = Grants::new(&[("ghp_elm", EXAMPLE_REPO)]);
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let (_e, elm) = rooms.connect(host("elm"));
        let start = Instant::now();
        for _ in 0..POST_BURST * 4 {
            assert_eq!(
                elm.post_at(&map(), &example_repo, MessageKind::Claim, widest(), start)
                    .await,
                Posted::NotMember
            );
        }

        join(&elm, &directory, "ghp_elm", example_repo.clone()).await;
        for n in 1..=widest_that_fit() {
            assert_eq!(
                elm.post_at(&map(), &example_repo, MessageKind::Claim, widest(), start)
                    .await,
                Posted::Delivered {
                    seq: RoomSeq(n),
                    recipients: 1
                }
            );
        }
    }

    /// A registry with the smallest turn cap, so a capped exchange is a few
    /// posts, and every other limit at its default.
    fn capped_rooms() -> Rooms {
        Rooms::with_limits(TokensPerHour::DEFAULT, TurnCap::MIN, Arc::new(Fake::new()))
    }

    fn turn_cap() -> u64 {
        u64::from(TurnCap::MIN.get())
    }

    /// `a` and `b` alternate short posts to `room` until they have exchanged
    /// exactly the cap, each one delivered. Returns the next host to post,
    /// which is the one the cap refuses.
    async fn exchange_to_the_cap<'m, 'r>(
        a: &'m Membership<'r>,
        b: &'m Membership<'r>,
        room: &RoomId,
        at: Instant,
    ) -> &'m Membership<'r> {
        for n in 1..=turn_cap() {
            let poster = if n % 2 == 1 { a } else { b };
            assert!(
                matches!(
                    poster
                        .post_at(&map(), room, MessageKind::Question, body("and you?"), at)
                        .await,
                    Posted::Delivered { seq: RoomSeq(s), .. } if s == n
                ),
                "post {n} of the exchange was not delivered"
            );
        }
        if turn_cap().is_multiple_of(2) { a } else { b }
    }

    #[tokio::test]
    async fn a_two_host_exchange_is_refused_past_the_cap_and_the_other_host_is_told_once() {
        let rooms = capped_rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        join(&bay, &directory, "ghp_bay", room.clone()).await;
        let start = Instant::now();

        let next = exchange_to_the_cap(&atlas, &bay, &room, start).await;
        assert_eq!(
            next.host,
            host("atlas"),
            "the cap is even, so atlas posts next"
        );
        assert_eq!(
            drain(&mut atlas_inbox).len(),
            usize::try_from(turn_cap()).unwrap()
        );
        assert_eq!(
            drain(&mut bay_inbox).len(),
            usize::try_from(turn_cap()).unwrap()
        );

        assert_eq!(
            atlas
                .post_at(&map(), &room, MessageKind::Ack, body("well"), start)
                .await,
            refused(&room, PostRefusal::TurnCapped)
        );
        assert!(
            drain(&mut atlas_inbox).is_empty(),
            "the poster's refusal is the answer"
        );
        assert_eq!(
            drain(&mut bay_inbox),
            vec![ServerFrame::TurnCapped { room: room.clone() }],
            "the other participant was not told"
        );

        // Both are refused from now on, and nobody is told again.
        for _ in 0..POST_BURST * 2 {
            for member in [&atlas, &bay] {
                assert_eq!(
                    member
                        .post_at(&map(), &room, MessageKind::Ack, body("?"), start)
                        .await,
                    refused(&room, PostRefusal::TurnCapped)
                );
            }
        }
        assert!(drain(&mut atlas_inbox).is_empty());
        assert!(drain(&mut bay_inbox).is_empty());
    }

    #[tokio::test]
    async fn a_capped_post_spends_no_sequence_number_rate_or_allowance() {
        let rooms = capped_rooms();
        let directory = Grants::new(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_bay", EXAMPLE_REPO),
            ("ghp_cedar", EXAMPLE_REPO),
        ]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (_b, bay) = rooms.connect(host("bay"));
        let (_c, cedar) = rooms.connect(host("cedar"));
        for (member, tok) in [
            (&atlas, "ghp_atlas"),
            (&bay, "ghp_bay"),
            (&cedar, "ghp_cedar"),
        ] {
            join(member, &directory, tok, room.clone()).await;
        }
        let start = Instant::now();
        exchange_to_the_cap(&atlas, &bay, &room, start).await;

        // More refused posts than the burst, each of them the most expensive
        // post there is. Spending the rate would turn them into `rate_limited`
        // part way; spending the allowance, into `budget_exhausted`.
        for i in 0..POST_BURST + 2 {
            assert_eq!(
                atlas
                    .post_at(&map(), &room, MessageKind::Claim, widest(), start)
                    .await,
                refused(&room, PostRefusal::TurnCapped),
                "refused post {i}"
            );
        }

        assert_eq!(
            cedar
                .post_at(
                    &map(),
                    &room,
                    MessageKind::Handoff,
                    body("over to atlas"),
                    start
                )
                .await,
            Posted::Delivered {
                seq: RoomSeq(turn_cap() + 1),
                recipients: 3
            },
            "a refused post used a sequence number"
        );
        // atlas's rate and allowance are what they were before her refusals:
        // every widest post the allowance covers, all inside her burst.
        for n in 0..widest_that_fit() {
            assert_eq!(
                atlas
                    .post_at(&map(), &room, MessageKind::Claim, widest(), start)
                    .await,
                Posted::Delivered {
                    seq: RoomSeq(turn_cap() + 2 + n),
                    recipients: 3
                },
                "a refused post spent atlas's rate or allowance"
            );
        }
    }

    #[tokio::test]
    async fn a_third_host_posting_lifts_the_cap_for_both_and_its_refused_post_does_not() {
        let rooms = capped_rooms();
        let directory = Grants::new(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_bay", EXAMPLE_REPO),
            ("ghp_cedar", EXAMPLE_REPO),
            ("ghp_cedar", SKILLS),
        ]);
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let skills = repo_room("example-tools", SKILLS);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (_b, bay) = rooms.connect(host("bay"));
        let (_c, cedar) = rooms.connect(host("cedar"));
        join(&atlas, &directory, "ghp_atlas", example_repo.clone()).await;
        join(&bay, &directory, "ghp_bay", example_repo.clone()).await;
        join(&cedar, &directory, "ghp_cedar", example_repo.clone()).await;
        join(&cedar, &directory, "ghp_cedar", skills.clone()).await;
        let start = Instant::now();
        exchange_to_the_cap(&atlas, &bay, &example_repo, start).await;

        // cedar has used her burst elsewhere, so her post here is refused by
        // the rate. A post that reaches no one is not a third party.
        for _ in 0..POST_BURST {
            cedar
                .post_at(&map(), &skills, MessageKind::Claim, body("busy"), start)
                .await;
        }
        assert_eq!(
            cedar
                .post_at(
                    &map(),
                    &example_repo,
                    MessageKind::Question,
                    body("stuck?"),
                    start
                )
                .await,
            refused(&example_repo, PostRefusal::RateLimited)
        );
        assert_eq!(
            atlas
                .post_at(&map(), &example_repo, MessageKind::Ack, body("?"), start)
                .await,
            refused(&example_repo, PostRefusal::TurnCapped)
        );

        let later = start + POST_REFILL;
        assert!(matches!(
            cedar
                .post_at(
                    &map(),
                    &example_repo,
                    MessageKind::Question,
                    body("stuck?"),
                    later
                )
                .await,
            Posted::Delivered { .. }
        ));
        for member in [&bay, &atlas] {
            assert!(
                matches!(
                    member
                        .post_at(&map(), &example_repo, MessageKind::Ack, body("yes"), later)
                        .await,
                    Posted::Delivered { .. }
                ),
                "a third host's post did not lift the cap"
            );
        }
    }

    #[tokio::test]
    async fn no_leave_rejoin_re_form_reconnect_or_revocation_resets_a_capped_exchange() {
        let rooms = capped_rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (_b, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        join(&bay, &directory, "ghp_bay", room.clone()).await;
        let start = Instant::now();
        exchange_to_the_cap(&atlas, &bay, &room, start).await;

        let still_capped = async |member: &Membership<'_>, after: &str| {
            assert_eq!(
                member
                    .post_at(&map(), &room, MessageKind::Claim, body("again"), start)
                    .await,
                refused(&room, PostRefusal::TurnCapped),
                "{} was let back in after {after}",
                member.host
            );
        };

        // One participant leaves and rejoins.
        assert!(bay.leave(&map(), &room).is_some());
        join(&bay, &directory, "ghp_bay", room.clone()).await;
        still_capped(&bay, "a leave and rejoin").await;
        still_capped(&atlas, "the other's leave and rejoin").await;

        // Both leave, so the room is forgotten, and re-form it.
        assert!(atlas.leave(&map(), &room).is_some());
        assert!(bay.leave(&map(), &room).is_some());
        assert_eq!(rooms.room_count(), 0, "the room did not empty");
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        join(&bay, &directory, "ghp_bay", room.clone()).await;
        still_capped(&atlas, "the room re-formed").await;
        still_capped(&bay, "the room re-formed").await;

        // bay reconnects.
        drop(bay);
        let (_b, bay) = rooms.connect(host("bay"));
        join(&bay, &directory, "ghp_bay", room.clone()).await;
        still_capped(&bay, "a reconnect").await;

        // bay's access is revoked, so he is removed; atlas is still capped.
        directory.revoke("ghp_bay", EXAMPLE_REPO);
        assert_eq!(
            bay.revalidate(&directory, &map(), &token("ghp_bay")).await,
            vec![ServerFrame::Left { room: room.clone() }]
        );
        still_capped(&atlas, "the other participant's revocation").await;
    }

    #[tokio::test]
    async fn a_turn_cap_counts_one_room_however_its_repo_is_spelled() {
        let rooms = capped_rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let spellings = [
            repo_room("example-repo", "Example-org/Example-repo"),
            room.clone(),
            repo_room("example-repo", "example-org/EXAMPLE-REPO"),
        ];
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (_b, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        join(&bay, &directory, "ghp_bay", room.clone()).await;
        let start = Instant::now();

        for n in 0..turn_cap() {
            let poster = if n % 2 == 0 { &atlas } else { &bay };
            let spelled = &spellings[usize::try_from(n).unwrap() % spellings.len()];
            assert!(matches!(
                poster
                    .post_at(&map(), spelled, MessageKind::Claim, body("mine"), start)
                    .await,
                Posted::Delivered { .. }
            ));
        }
        for spelled in &spellings {
            assert_eq!(
                atlas
                    .post_at(&map(), spelled, MessageKind::Claim, body("mine"), start)
                    .await,
                refused(&room, PostRefusal::TurnCapped),
                "{spelled:?} counted as a different room"
            );
        }
    }

    #[tokio::test]
    async fn a_turn_cap_notice_is_queued_only_for_the_other_hosts_connections_in_the_room() {
        // The review's surviving mutation M17a: a notice sent to any
        // connection of the other host, which only `deliverable` hid. This
        // reads the raw queue, so the targeting itself has to be right.
        let rooms = capped_rooms();
        let directory = Grants::new(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_bay", EXAMPLE_REPO),
            ("ghp_bay", SKILLS),
        ]);
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let skills = repo_room("example-tools", SKILLS);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", example_repo.clone()).await;
        join(&bay, &directory, "ghp_bay", example_repo.clone()).await;
        join(&bay, &directory, "ghp_bay", skills.clone()).await;
        let start = Instant::now();
        exchange_to_the_cap(&atlas, &bay, &example_repo, start).await;

        // bay's connection leaves the room but stays in another one, and a
        // second connection of bay's holds only that other room.
        assert!(bay.leave(&map(), &example_repo).is_some());
        let (mut elsewhere_inbox, elsewhere) = rooms.connect(host("bay"));
        join(&elsewhere, &directory, "ghp_bay", skills.clone()).await;
        drain(&mut bay_inbox);

        assert_eq!(
            atlas
                .post_at(
                    &map(),
                    &example_repo,
                    MessageKind::Claim,
                    body("over"),
                    start
                )
                .await,
            refused(&example_repo, PostRefusal::TurnCapped)
        );
        for (name, inbox) in [("bay", &mut bay_inbox), ("elsewhere", &mut elsewhere_inbox)] {
            assert!(
                !drain(inbox)
                    .iter()
                    .any(|frame| matches!(frame, ServerFrame::TurnCapped { .. })),
                "a notice was queued for {name}, a connection not in the room"
            );
        }
    }

    #[tokio::test]
    async fn a_host_that_posted_once_cannot_be_muted_by_another_members_monologue() {
        // The review finding on PR #98, in the registry: cedar posts once,
        // magenta posts status updates, and cedar must still be able to post.
        for cap in [TurnCap::MIN, TurnCap::DEFAULT] {
            let rooms = Rooms::with_limits(TokensPerHour::DEFAULT, cap, Arc::new(Fake::new()));
            let directory =
                Grants::new(&[("ghp_cedar", EXAMPLE_REPO), ("ghp_magenta", EXAMPLE_REPO)]);
            let room = repo_room("example-repo", EXAMPLE_REPO);
            let (mut cedar_inbox, cedar) = rooms.connect(host("cedar"));
            let (_m, magenta) = rooms.connect(host("magenta"));
            join(&cedar, &directory, "ghp_cedar", room.clone()).await;
            join(&magenta, &directory, "ghp_magenta", room.clone()).await;
            let start = Instant::now();

            assert!(matches!(
                cedar
                    .post_at(&map(), &room, MessageKind::Claim, body("taking it"), start)
                    .await,
                Posted::Delivered { .. }
            ));
            // One post per refill, so the rate never refuses, and short ones,
            // so the allowance never does. Only the turn cap could.
            let updates = cap.get() * 3;
            for i in 1..=updates {
                let posted = magenta
                    .post_at(
                        &map(),
                        &room,
                        MessageKind::Claim,
                        body("status"),
                        start + POST_REFILL * i,
                    )
                    .await;
                assert!(
                    matches!(posted, Posted::Delivered { .. }),
                    "{cap:?}: magenta's update {i}: {posted:?}"
                );
            }
            let after = start + POST_REFILL * (updates + 1);
            let posted = cedar
                .post_at(&map(), &room, MessageKind::Ack, body("done"), after)
                .await;
            assert!(
                matches!(posted, Posted::Delivered { .. }),
                "{cap:?}: cedar was muted by magenta's monologue: {posted:?}"
            );
            assert!(
                !drain(&mut cedar_inbox)
                    .iter()
                    .any(|frame| matches!(frame, ServerFrame::TurnCapped { .. })),
                "{cap:?}: cedar was told she was capped"
            );
        }
    }

    #[tokio::test]
    async fn a_turn_cap_notice_is_not_sent_for_a_room_the_member_has_left() {
        let rooms = capped_rooms();
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        join(&bay, &directory, "ghp_bay", room.clone()).await;
        let start = Instant::now();
        exchange_to_the_cap(&atlas, &bay, &room, start).await;
        // bay's socket has sent everything so far.
        assert_eq!(
            still_sendable(&bay, &mut bay_inbox).await.len(),
            usize::try_from(turn_cap()).unwrap()
        );

        atlas
            .post_at(&map(), &room, MessageKind::Claim, body("over"), start)
            .await;
        assert!(bay.leave(&map(), &room).is_some());
        assert!(
            still_sendable(&bay, &mut bay_inbox).await.is_empty(),
            "a notice for a room bay left went out after his Left"
        );
    }

    // ---- Room history at rest ----

    #[tokio::test]
    async fn a_delivered_post_is_recorded_with_its_sequence_and_a_seven_day_expiry() {
        let store = Arc::new(Fake::new());
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;

        assert_eq!(
            atlas
                .post_at(
                    &map(),
                    &room,
                    MessageKind::Claim,
                    body("taking ENG-557"),
                    Instant::now()
                )
                .await,
            Posted::Delivered {
                seq: RoomSeq(1),
                recipients: 1
            }
        );

        assert_eq!(store.seqs_of(&room), vec![1]);
        assert_eq!(store.bodies_of(&room), vec!["taking ENG-557".to_owned()]);
        // Exactly seven days past the post, and the table does the deleting.
        assert_eq!(
            store.expires_at_of(&room, 1),
            Some(test_posted_at() + RETENTION.as_secs())
        );
        assert_eq!(bodies(&drain(&mut atlas_inbox)), vec!["taking ENG-557"]);
    }

    #[tokio::test]
    async fn a_room_that_re_forms_continues_its_numbering_from_stored_history() {
        // The carry-forward from the XYZ-59 review: `RoomSeq` restarts when a
        // room empties and re-forms, and after every deploy. Five messages are
        // already in the table and this registry has never seen the room.
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let store = Arc::new(Fake::seeded(&room, 5, &host("atlas")));
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO)]);
        let (_a, atlas) = rooms.connect(host("atlas"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;

        assert_eq!(
            atlas
                .post_at(
                    &map(),
                    &room,
                    MessageKind::Claim,
                    body("after the re-form"),
                    Instant::now()
                )
                .await,
            Posted::Delivered {
                seq: RoomSeq(6),
                recipients: 1
            },
            "the room numbered from 1 over the top of its own history"
        );

        // The seed is read once per room, not once per post.
        assert_eq!(store.seq_reads(), 1);
        assert!(matches!(
            atlas
                .post_at(
                    &map(),
                    &room,
                    MessageKind::Claim,
                    body("and again"),
                    Instant::now()
                )
                .await,
            Posted::Delivered {
                seq: RoomSeq(7),
                ..
            }
        ));
        assert_eq!(store.seq_reads(), 1);
        assert_eq!(store.seqs_of(&room), vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[tokio::test]
    async fn a_write_failure_refuses_the_post_and_delivers_it_to_nobody() {
        let store = Arc::new(Fake::failing());
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        join(&bay, &directory, "ghp_bay", room.clone()).await;

        assert_eq!(
            atlas
                .post_at(
                    &map(),
                    &room,
                    MessageKind::Claim,
                    body("lost"),
                    Instant::now()
                )
                .await,
            refused(&room, PostRefusal::NotRecorded)
        );
        assert_eq!(store.appends(), 1, "the write was never attempted");
        for (name, inbox) in [
            ("the poster", &mut atlas_inbox),
            ("another member", &mut bay_inbox),
        ] {
            assert!(
                drain(inbox).is_empty(),
                "a post that was not recorded was delivered to {name}"
            );
        }
    }

    #[tokio::test]
    async fn a_sequence_collision_is_refused_and_its_number_is_not_reused() {
        // The case the conditional write exists for: a number the live counter
        // believes is free that the table already holds.
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let store = Arc::new(Fake::colliding_at(1));
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO)]);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;

        assert_eq!(
            atlas
                .post_at(
                    &map(),
                    &room,
                    MessageKind::Claim,
                    body("collides"),
                    Instant::now()
                )
                .await,
            refused(&room, PostRefusal::NotRecorded),
            "a colliding write was not refused"
        );
        assert!(drain(&mut atlas_inbox).is_empty());
        assert!(
            store.bodies_of(&room).is_empty(),
            "a colliding write stored or overwrote something"
        );

        // Burned, not rolled back: the next post takes 2 and lands, so a write
        // that landed with its response lost cannot wedge the room.
        assert_eq!(
            atlas
                .post_at(
                    &map(),
                    &room,
                    MessageKind::Claim,
                    body("next"),
                    Instant::now()
                )
                .await,
            Posted::Delivered {
                seq: RoomSeq(2),
                recipients: 1
            }
        );
        assert_eq!(store.seqs_of(&room), vec![2]);
    }

    #[tokio::test]
    async fn a_seed_that_cannot_be_read_refuses_the_post_rather_than_numbering_from_one() {
        let store = Arc::new(Fake::unreadable());
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;

        assert_eq!(
            atlas
                .post_at(
                    &map(),
                    &room,
                    MessageKind::Claim,
                    body("unnumbered"),
                    Instant::now()
                )
                .await,
            refused(&room, PostRefusal::NotRecorded)
        );
        assert!(drain(&mut atlas_inbox).is_empty());
        assert_eq!(
            store.appends(),
            0,
            "a message was written without a seeded sequence"
        );
    }

    #[tokio::test]
    async fn a_rooms_history_reads_back_in_the_order_it_was_delivered() {
        let store = Arc::new(Fake::new());
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;

        let start = Instant::now();
        for (i, text) in ["one", "two", "three", "four"].iter().enumerate() {
            let at = start + POST_REFILL * u32::try_from(i).unwrap();
            assert!(matches!(
                atlas
                    .post_at(&map(), &room, MessageKind::Claim, body(text), at)
                    .await,
                Posted::Delivered { .. }
            ));
        }

        let read = backfill(&*store, &room, None).await.unwrap();
        assert_eq!(
            read.messages
                .iter()
                .map(|record| (record.seq.0, record.body.to_string()))
                .collect::<Vec<_>>(),
            vec![
                (1, "one".to_owned()),
                (2, "two".to_owned()),
                (3, "three".to_owned()),
                (4, "four".to_owned()),
            ]
        );
        // The order history has is the order the sockets saw.
        assert_eq!(
            bodies(&drain(&mut atlas_inbox)),
            vec!["one", "two", "three", "four"]
        );

        // And it pages, in order, without losing or repeating a message.
        let first = store.page(&room, None, 2).await.unwrap();
        assert_eq!(
            first
                .messages
                .iter()
                .map(|record| record.seq.0)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let second = store.page(&room, first.next, 2).await.unwrap();
        assert_eq!(
            second
                .messages
                .iter()
                .map(|record| record.seq.0)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
    }

    #[tokio::test]
    async fn a_post_to_a_room_this_connection_is_not_in_reads_and_writes_nothing() {
        // Rule 44: a frame from a peer must not be able to make the router
        // touch the store for a room it has no business in.
        let store = Arc::new(Fake::new());
        let rooms = rooms_over(Arc::clone(&store));
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_e, elm) = rooms.connect(host("elm"));

        for forged in [
            room.clone(),
            project_room("example-repo"),
            project_room("ghost"),
        ] {
            assert_eq!(
                elm.post_at(
                    &map(),
                    &forged,
                    MessageKind::Claim,
                    body("forged"),
                    Instant::now()
                )
                .await,
                Posted::NotMember,
                "{forged:?}"
            );
        }
        assert_eq!(store.appends(), 0);
        assert_eq!(
            store.seq_reads(),
            0,
            "a non-member's post made the router read the store"
        );
    }

    #[tokio::test]
    async fn a_failed_seed_is_not_retried_by_every_post_until_the_backoff_passes() {
        // Without the backoff, a failed seed leaves the room unseeded, so
        // every later post re-attempts the read and pays the whole persist
        // budget with the room's ingest lock held — an unreachable table would
        // queue every post to the room behind a five-second timeout,
        // repeatedly.
        let store = Arc::new(Fake::unreadable());
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;

        let start = Instant::now();
        assert_eq!(
            atlas
                .post_at(&map(), &room, MessageKind::Claim, body("one"), start)
                .await,
            refused(&room, PostRefusal::NotRecorded)
        );
        assert_eq!(store.seq_reads(), 1);

        // Every post inside the window is refused without asking again. More
        // of them than the post burst, since a seed refusal spends no rate.
        for i in 1..=POST_BURST + 2 {
            assert_eq!(
                atlas
                    .post_at(
                        &map(),
                        &room,
                        MessageKind::Claim,
                        body("again"),
                        start + STORE_BACKOFF / 2
                    )
                    .await,
                refused(&room, PostRefusal::NotRecorded),
                "post {i} inside the backoff"
            );
        }
        assert_eq!(
            store.seq_reads(),
            1,
            "a post inside the backoff read the store again"
        );

        // Once the window has passed, one post tries again.
        assert_eq!(
            atlas
                .post_at(
                    &map(),
                    &room,
                    MessageKind::Claim,
                    body("later"),
                    start + STORE_BACKOFF
                )
                .await,
            refused(&room, PostRefusal::NotRecorded)
        );
        assert_eq!(store.seq_reads(), 2, "the backoff window never expired");

        assert!(drain(&mut atlas_inbox).is_empty());
        assert_eq!(store.appends(), 0, "an unseeded room wrote a message");
    }

    #[tokio::test]
    async fn two_concurrent_posts_are_delivered_in_the_order_history_has_them() {
        // The ordering guarantee the ingest lock exists for, which nothing
        // pinned before: atlas's write is slow and bay's is instant, so if the
        // lock were released between taking a sequence number and writing,
        // bay's seq 2 would reach the sockets before atlas's seq 1 while the
        // table read 1 then 2.
        let store = Arc::new(Fake::slow_first(Duration::from_millis(80)));
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        let (_b, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        join(&bay, &directory, "ghp_bay", room.clone()).await;

        let projects = map();
        let now = Instant::now();
        // `join!` polls in order, so atlas takes the room's ingest lock first
        // and gets seq 1; bay waits for it, because a tokio mutex is fair.
        let (first, second) = tokio::join!(
            atlas.post_at(&projects, &room, MessageKind::Claim, body("slow"), now),
            bay.post_at(&projects, &room, MessageKind::Claim, body("fast"), now),
        );
        assert!(
            matches!(
                first,
                Posted::Delivered {
                    seq: RoomSeq(1),
                    ..
                }
            ),
            "{first:?}"
        );
        assert!(
            matches!(
                second,
                Posted::Delivered {
                    seq: RoomSeq(2),
                    ..
                }
            ),
            "{second:?}"
        );

        // Delivered in sequence order, and the table agrees with the sockets.
        assert_eq!(bodies(&drain(&mut atlas_inbox)), vec!["slow", "fast"]);
        assert_eq!(store.seqs_of(&room), vec![1, 2]);
        assert_eq!(
            backfill(&*store, &room, None)
                .await
                .unwrap()
                .messages
                .iter()
                .map(|record| record.body.to_string())
                .collect::<Vec<_>>(),
            vec!["slow".to_owned(), "fast".to_owned()]
        );
    }

    #[tokio::test]
    async fn a_write_slower_than_the_budget_refuses_the_post_and_delivers_nothing() {
        // The budget path had no coverage at all: a store that is slow rather
        // than down is the case a timeout exists for.
        let store = Arc::new(Fake::slow(Duration::from_millis(400)));
        let mut rooms = rooms_over(Arc::clone(&store));
        rooms.set_persist_budget(Duration::from_millis(20));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;

        assert_eq!(
            atlas
                .post_at(
                    &map(),
                    &room,
                    MessageKind::Claim,
                    body("too slow"),
                    Instant::now()
                )
                .await,
            refused(&room, PostRefusal::NotRecorded),
            "a write past the budget was not refused"
        );
        assert!(
            drain(&mut atlas_inbox).is_empty(),
            "a post whose write timed out was delivered"
        );
    }

    // ---- Archive on merge ----

    /// Grants every read, and answers every repo's pull request listing with
    /// the same list (or the same failure). Records which token asked about
    /// which repo. With a gate, each listing signals `started` and then waits
    /// for the gate to open, so a test can hold a check in flight.
    struct Merges {
        pulls: Mutex<Result<Vec<crate::auth::PullSummary>, crate::auth::PullsUnverifiable>>,
        asked: Mutex<Vec<(String, String)>>,
        started: tokio::sync::Notify,
        gate: Option<tokio::sync::Notify>,
    }

    impl Merges {
        fn answering(
            pulls: Result<Vec<crate::auth::PullSummary>, crate::auth::PullsUnverifiable>,
        ) -> Self {
            Self {
                pulls: Mutex::new(pulls),
                asked: Mutex::new(Vec::new()),
                started: tokio::sync::Notify::new(),
                gate: None,
            }
        }

        fn merged(head: &str) -> Self {
            Self::answering(Ok(vec![pull(head, crate::auth::PullState::Merged)]))
        }

        fn gated(mut self) -> Self {
            self.gate = Some(tokio::sync::Notify::new());
            self
        }

        fn set(&self, pulls: Vec<crate::auth::PullSummary>) {
            *self.pulls.lock().unwrap() = Ok(pulls);
        }

        fn calls(&self) -> usize {
            self.asked.lock().unwrap().len()
        }
    }

    impl Directory for Merges {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn can_read(&self, _token: &SecretToken, _repo: &RepoSlug) -> RepoAccess {
            RepoAccess::Readable
        }
        async fn recent_pulls(
            &self,
            token: &SecretToken,
            repo: &RepoSlug,
        ) -> Result<Vec<crate::auth::PullSummary>, crate::auth::PullsUnverifiable> {
            self.asked
                .lock()
                .unwrap()
                .push((token.expose().to_owned(), repo.as_str().to_owned()));
            if let Some(gate) = &self.gate {
                self.started.notify_one();
                gate.notified().await;
            }
            self.pulls.lock().unwrap().clone()
        }
    }

    fn pull(head: &str, state: crate::auth::PullState) -> crate::auth::PullSummary {
        crate::auth::PullSummary {
            head_ref: head.to_owned(),
            into_default_branch: true,
            state,
        }
    }

    async fn post_as(
        member: &Membership<'_>,
        directory: &Merges,
        tok: &str,
        room: &RoomId,
        text: &str,
    ) -> Posted {
        member
            .post(
                directory,
                &map(),
                &token(tok),
                room,
                MessageKind::Claim,
                body(text),
            )
            .await
    }

    fn is_delivered(posted: &Posted) -> bool {
        matches!(posted, Posted::Delivered { .. })
    }

    #[tokio::test]
    async fn a_merged_ticket_room_refuses_writes_and_still_serves_its_history_and_joins() {
        let store = Arc::new(Fake::new());
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Merges::answering(Ok(Vec::new()));
        let room = ticket_room("example-repo", 12);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        atlas
            .join(&directory, &map(), &token("ghp_atlas"), room.clone())
            .await;

        // Open while the pull request has not merged.
        let before = post_as(&atlas, &directory, "ghp_atlas", &room, "before").await;
        assert!(is_delivered(&before), "{before:?}");
        assert_eq!(bodies(&drain(&mut atlas_inbox)), vec!["before"]);

        directory.set(vec![pull(
            "feature/eng-12-portal",
            crate::auth::PullState::Merged,
        )]);
        // "Not merged" is believed for its TTL, so the merge is seen by the
        // first post after that.
        let later = Instant::now().checked_add(archive::NOT_MERGED_TTL).unwrap();
        atlas
            .settle_archive(&directory, &map(), &token("ghp_atlas"), &room, later)
            .await;
        assert!(rooms.is_archived(&room));

        assert_eq!(
            post_as(&atlas, &directory, "ghp_atlas", &room, "after").await,
            refused(&room, PostRefusal::Archived),
            "an archived room took a write"
        );
        assert!(
            drain(&mut atlas_inbox).is_empty(),
            "an archived room delivered"
        );

        // Nothing deleted: the history still has what was posted before.
        let read = backfill(&*store, &room, None).await.unwrap();
        let stored: Vec<&str> = read.messages.iter().map(|r| r.body.as_str()).collect();
        assert_eq!(stored, vec!["before"]);

        // Joins still work, and the newcomer is refused writes too.
        let (_bay_inbox, bay) = rooms.connect(host("bay"));
        assert_eq!(
            bay.join(&directory, &map(), &token("ghp_bay"), room.clone())
                .await,
            ServerFrame::Joined { room: room.clone() }
        );
        assert_eq!(
            post_as(&bay, &directory, "ghp_bay", &room, "late").await,
            refused(&room, PostRefusal::Archived)
        );
    }

    #[tokio::test]
    async fn an_archived_room_reopens_when_a_pull_request_for_its_ticket_opens() {
        // Part two of a stacked ticket, opened after part one merged. Only a
        // post once the archive's re-check is due asks again.
        let rooms = rooms();
        let directory = Merges::merged("feature/eng-12-part-1");
        let room = ticket_room("example-repo", 12);
        let (mut inbox, atlas) = rooms.connect(host("atlas"));
        atlas
            .join(&directory, &map(), &token("ghp_atlas"), room.clone())
            .await;
        assert_eq!(
            post_as(&atlas, &directory, "ghp_atlas", &room, "x").await,
            refused(&room, PostRefusal::Archived)
        );

        directory.set(vec![
            pull("feature/eng-12-part-1", crate::auth::PullState::Merged),
            pull("feature/eng-12-part-2", crate::auth::PullState::Open),
        ]);
        // Not yet due: still archived, and GitHub is not asked.
        let calls = directory.calls();
        assert_eq!(
            post_as(&atlas, &directory, "ghp_atlas", &room, "y").await,
            refused(&room, PostRefusal::Archived)
        );
        assert_eq!(directory.calls(), calls);

        let due = Instant::now()
            .checked_add(archive::ARCHIVED_RECHECK)
            .unwrap();
        atlas
            .settle_archive(&directory, &map(), &token("ghp_atlas"), &room, due)
            .await;
        assert!(
            !rooms.is_archived(&room),
            "part two did not reopen the room"
        );
        let posted = post_as(&atlas, &directory, "ghp_atlas", &room, "part two").await;
        assert!(is_delivered(&posted), "{posted:?}");
        assert_eq!(bodies(&drain(&mut inbox)), vec!["part two"]);
    }

    #[tokio::test]
    async fn a_room_that_empties_and_re_forms_is_still_archived() {
        let rooms = rooms();
        let directory = Merges::merged("feature/eng-12-portal");
        let room = ticket_room("example-repo", 12);
        {
            let (_inbox, atlas) = rooms.connect(host("atlas"));
            atlas
                .join(&directory, &map(), &token("ghp_atlas"), room.clone())
                .await;
            post_as(&atlas, &directory, "ghp_atlas", &room, "x").await;
        }
        assert_eq!(rooms.room_count(), 0);
        let (_inbox, bay) = rooms.connect(host("bay"));
        bay.join(&directory, &map(), &token("ghp_bay"), room.clone())
            .await;
        let calls = directory.calls();
        assert_eq!(
            post_as(&bay, &directory, "ghp_bay", &room, "y").await,
            refused(&room, PostRefusal::Archived)
        );
        assert_eq!(directory.calls(), calls, "a sticky verdict asked again");
    }

    #[tokio::test]
    async fn an_unverifiable_check_leaves_the_room_open() {
        let rooms = rooms();
        let directory = Merges::answering(Err(crate::auth::PullsUnverifiable));
        let room = ticket_room("example-repo", 12);
        let (mut inbox, atlas) = rooms.connect(host("atlas"));
        atlas
            .join(&directory, &map(), &token("ghp_atlas"), room.clone())
            .await;

        let posted = post_as(&atlas, &directory, "ghp_atlas", &room, "still open").await;
        assert!(is_delivered(&posted), "{posted:?}");
        assert!(!rooms.is_archived(&room));
        assert_eq!(bodies(&drain(&mut inbox)), vec!["still open"]);
    }

    #[tokio::test]
    async fn a_check_past_its_deadline_leaves_the_room_open_and_is_retried() {
        let mut rooms = rooms();
        rooms.set_merge_budget(Duration::from_millis(20));
        // Gated and never opened: GitHub never answers.
        let directory = Merges::merged("feature/eng-12-portal").gated();
        let room = ticket_room("example-repo", 12);
        let (_inbox, atlas) = rooms.connect(host("atlas"));
        atlas
            .join(&directory, &map(), &token("ghp_atlas"), room.clone())
            .await;

        let posted = post_as(&atlas, &directory, "ghp_atlas", &room, "x").await;
        assert!(is_delivered(&posted), "a timed-out check closed the room");
        assert!(!rooms.is_archived(&room));

        // Remembered as unverifiable, not as "not merged": asked again once
        // the short retry has passed.
        let calls = directory.calls();
        let retry = Instant::now()
            .checked_add(archive::UNVERIFIABLE_RETRY)
            .unwrap();
        atlas
            .settle_archive(&directory, &map(), &token("ghp_atlas"), &room, retry)
            .await;
        assert!(directory.calls() > calls, "an unverifiable verdict stuck");
    }

    #[tokio::test]
    async fn repeat_archiving_is_a_no_op_and_asks_github_once() {
        let rooms = rooms();
        let directory = Merges::merged("feature/eng-12-portal");
        let room = ticket_room("example-repo", 12);
        let (_inbox, atlas) = rooms.connect(host("atlas"));
        atlas
            .join(&directory, &map(), &token("ghp_atlas"), room.clone())
            .await;

        for _ in 0..3 {
            assert_eq!(
                post_as(&atlas, &directory, "ghp_atlas", &room, "x").await,
                refused(&room, PostRefusal::Archived)
            );
        }
        // Example-repo lists two repos: one check, one request per repo.
        assert_eq!(directory.calls(), 2);
        assert!(rooms.is_archived(&room));
    }

    #[tokio::test]
    async fn project_and_repo_rooms_are_never_archived_and_ask_github_nothing() {
        let rooms = rooms();
        // Everything reads as merged, so only the scope rule keeps these open.
        let directory = Merges::merged("feature/eng-12-portal");
        let (mut inbox, atlas) = rooms.connect(host("atlas"));
        for room in [
            project_room("example-repo"),
            repo_room("example-repo", EXAMPLE_REPO),
        ] {
            atlas
                .join(&directory, &map(), &token("ghp_atlas"), room.clone())
                .await;
            for _ in 0..2 {
                let posted = post_as(&atlas, &directory, "ghp_atlas", &room, "open").await;
                assert!(is_delivered(&posted), "{room:?}: {posted:?}");
            }
            assert!(!rooms.is_archived(&room));
        }
        assert_eq!(directory.calls(), 0, "a non-ticket room asked GitHub");
        assert_eq!(drain(&mut inbox).len(), 4);
    }

    #[tokio::test]
    async fn a_post_to_a_ticket_room_this_connection_is_not_in_asks_github_nothing() {
        let rooms = rooms();
        let directory = Merges::merged("feature/eng-12-portal");
        let room = ticket_room("example-repo", 12);
        let (_inbox, elm) = rooms.connect(host("elm"));
        assert_eq!(
            post_as(&elm, &directory, "ghp_elm", &room, "forged").await,
            Posted::NotMember
        );
        assert_eq!(directory.calls(), 0);
        assert!(!rooms.is_archived(&room));
    }

    #[tokio::test]
    async fn the_check_uses_the_rooms_ticket_and_projects_repos_with_the_posters_token() {
        // Rule 12: nothing the client sends picks the pull request. The only
        // inputs are the room's own ticket, the router's map for its project,
        // and the posting connection's credential.
        let rooms = rooms();
        // A merged pull request for a *different* ticket.
        let directory = Merges::merged("feature/eng-13-other");
        let room = ticket_room("example-repo", 12);
        let (_inbox, atlas) = rooms.connect(host("atlas"));
        atlas
            .join(&directory, &map(), &token("ghp_atlas"), room.clone())
            .await;

        let posted = post_as(&atlas, &directory, "ghp_atlas", &room, "x").await;
        assert!(
            is_delivered(&posted),
            "another ticket's merge closed this room"
        );
        assert_eq!(
            *directory.asked.lock().unwrap(),
            vec![
                ("ghp_atlas".to_owned(), EXAMPLE_REPO.to_owned()),
                ("ghp_atlas".to_owned(), PORTAL.to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn a_current_verdict_stops_the_per_post_github_call() {
        let rooms = rooms();
        let directory = Merges::answering(Ok(Vec::new()));
        let room = ticket_room("example-repo", 12);
        let (_inbox, atlas) = rooms.connect(host("atlas"));
        let (_inbox_b, bay) = rooms.connect(host("bay"));
        atlas
            .join(&directory, &map(), &token("ghp_atlas"), room.clone())
            .await;
        bay.join(&directory, &map(), &token("ghp_bay"), room.clone())
            .await;

        for text in ["one", "two", "three"] {
            assert!(is_delivered(
                &post_as(&atlas, &directory, "ghp_atlas", &room, text).await
            ));
        }
        assert!(is_delivered(
            &post_as(&bay, &directory, "ghp_bay", &room, "four").await
        ));
        // One check for the room, however many posts and posters.
        assert_eq!(directory.calls(), 2);
    }

    #[tokio::test]
    async fn an_archived_room_refuses_before_any_limit_is_spent() {
        let rooms = rooms();
        let directory = Merges::merged("feature/eng-12-portal");
        let ticket = ticket_room("example-repo", 12);
        let project = project_room("example-repo");
        let (_inbox, atlas) = rooms.connect(host("atlas"));
        atlas
            .join(&directory, &map(), &token("ghp_atlas"), ticket.clone())
            .await;
        atlas
            .join(&directory, &map(), &token("ghp_atlas"), project.clone())
            .await;

        // Far past a burst: if these spent the host's post rate, the post
        // to the project room below would be refused as rate limited.
        for _ in 0..POST_BURST.saturating_mul(2) {
            assert_eq!(
                post_as(&atlas, &directory, "ghp_atlas", &ticket, "x").await,
                refused(&ticket, PostRefusal::Archived)
            );
        }
        let posted = post_as(&atlas, &directory, "ghp_atlas", &project, "open").await;
        assert!(is_delivered(&posted), "{posted:?}");
    }

    #[tokio::test]
    async fn a_slow_merge_check_does_not_hold_up_a_post_to_another_room() {
        // The merge check must not hold up a post to another room. With one
        // ingest lock per room this no longer tells "before the lock" from
        // "under it" — that needs a same-room version — but it still pins
        // that a GitHub wait in one room stalls nobody else. Deterministic: atlas's check is provably in flight — the
        // directory signalled it and is parked on a gate nobody opens yet —
        // when bay posts, and bay's post has to finish well inside a window
        // the check cannot.
        let rooms = rooms();
        let directory = Merges::answering(Ok(Vec::new())).gated();
        let ticket = ticket_room("example-repo", 12);
        let project = project_room("example-repo");
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        let (_bay_inbox, bay) = rooms.connect(host("bay"));
        atlas
            .join(&directory, &map(), &token("ghp_atlas"), ticket.clone())
            .await;
        bay.join(&directory, &map(), &token("ghp_bay"), project.clone())
            .await;

        let atlas_post = post_as(&atlas, &directory, "ghp_atlas", &ticket, "waits");
        tokio::pin!(atlas_post);
        tokio::select! {
            posted = &mut atlas_post => panic!("the check never waited: {posted:?}"),
            () = directory.started.notified() => {}
        }

        let bay_posted = tokio::time::timeout(
            Duration::from_millis(500),
            post_as(&bay, &directory, "ghp_bay", &project, "not held up"),
        )
        .await
        .expect("a post to another room waited on a merge check");
        assert!(is_delivered(&bay_posted), "{bay_posted:?}");

        // Let the check finish: both of example-repo's repos, one at a time.
        let gate = directory.gate.as_ref().unwrap();
        gate.notify_one();
        tokio::select! {
            posted = &mut atlas_post => panic!("finished after one repo: {posted:?}"),
            () = directory.started.notified() => {}
        }
        gate.notify_one();
        let posted = atlas_post.await;
        assert!(is_delivered(&posted), "{posted:?}");
        assert_eq!(bodies(&drain(&mut atlas_inbox)), vec!["waits"]);
    }

    // ---- One ingest lock per room ----

    /// Polls `post` until the store has started more writes than `before`,
    /// failing if the post finishes first.
    async fn until_writing<F>(store: &Fake, before: u64, mut post: std::pin::Pin<&mut F>)
    where
        F: std::future::Future<Output = Posted>,
    {
        while store.appends() == before {
            tokio::select! {
                posted = post.as_mut() => panic!("the write finished before it was seen: {posted:?}"),
                () = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
        }
    }

    /// Polls `future` once: `Some` with its output when it is already done.
    async fn poll_once<F: std::future::Future>(
        mut future: std::pin::Pin<&mut F>,
    ) -> Option<F::Output> {
        std::future::poll_fn(|cx| {
            std::task::Poll::Ready(match future.as_mut().poll(cx) {
                std::task::Poll::Ready(output) => Some(output),
                std::task::Poll::Pending => None,
            })
        })
        .await
    }

    #[tokio::test]
    async fn a_slow_write_in_one_room_does_not_delay_a_post_to_another() {
        // The acceptance for XYZ-96: with one lock for the whole router, bay's
        // post to example-tools queued behind atlas's write to example-repo for as
        // long as that write took. Deterministic: atlas's write is provably in
        // flight, and far slower than the window bay's post must finish in.
        let store = Arc::new(Fake::slow_first(Duration::from_secs(3)));
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", SKILLS)]);
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let skills = repo_room("example-tools", SKILLS);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (mut bay_inbox, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", example_repo.clone()).await;
        join(&bay, &directory, "ghp_bay", skills.clone()).await;

        let projects = map();
        let slow = atlas.post_at(
            &projects,
            &example_repo,
            MessageKind::Claim,
            body("slow"),
            Instant::now(),
        );
        tokio::pin!(slow);
        until_writing(&store, 0, slow.as_mut()).await;

        let other = tokio::time::timeout(
            Duration::from_millis(500),
            bay.post_at(
                &projects,
                &skills,
                MessageKind::Claim,
                body("not held up"),
                Instant::now(),
            ),
        )
        .await
        .expect("a post to another room waited on a slow write");
        assert_eq!(
            other,
            Posted::Delivered {
                seq: RoomSeq(1),
                recipients: 1
            }
        );
        assert_eq!(bodies(&drain(&mut bay_inbox)), vec!["not held up"]);
        assert_eq!(store.seqs_of(&skills), vec![1]);
        assert!(
            store.seqs_of(&example_repo).is_empty(),
            "the slow write landed early"
        );

        let posted = slow.await;
        assert!(is_delivered(&posted), "{posted:?}");
        assert_eq!(store.seqs_of(&example_repo), vec![1]);
    }

    #[tokio::test]
    async fn a_room_that_reforms_while_its_write_is_in_flight_does_not_number_over_it() {
        // Why the locks are keyed by room in their own table rather than kept
        // on `Room`: a room is forgotten when it empties, so a lock kept on it
        // would be a fresh one when the room re-forms. The re-formed room's
        // first post would then seed before atlas's write lands, take seq 1
        // again, and one of the two would be refused as a collision.
        let store = Arc::new(Fake::slow_first(Duration::from_millis(300)));
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (_b, bay) = rooms.connect(host("bay"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;

        let projects = map();
        let slow = atlas.post_at(
            &projects,
            &room,
            MessageKind::Claim,
            body("first"),
            Instant::now(),
        );
        tokio::pin!(slow);
        until_writing(&store, 0, slow.as_mut()).await;

        // The room empties and is forgotten, then re-forms, mid-write.
        assert!(atlas.leave(&projects, &room).is_some());
        assert_eq!(rooms.room_count(), 0);
        join(&bay, &directory, "ghp_bay", room.clone()).await;

        let (first, second) = tokio::join!(
            &mut slow,
            bay.post_at(
                &projects,
                &room,
                MessageKind::Claim,
                body("second"),
                Instant::now()
            ),
        );
        assert!(
            matches!(
                first,
                Posted::Delivered {
                    seq: RoomSeq(1),
                    ..
                }
            ),
            "{first:?}"
        );
        assert!(
            matches!(
                second,
                Posted::Delivered {
                    seq: RoomSeq(2),
                    ..
                }
            ),
            "{second:?}"
        );
        assert_eq!(store.seqs_of(&room), vec![1, 2]);
        assert_eq!(store.bodies_of(&room), vec!["first", "second"]);
    }

    #[tokio::test]
    async fn the_ingest_lock_table_holds_a_room_only_while_a_post_uses_it() {
        // Rule 44: an entry is added by the first post into a room and removed
        // by the last to let go — finishing, or dropped while it waits or
        // writes — so the table never outlives the posts using it.
        let store = Arc::new(Fake::slow(Duration::from_millis(200)));
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_a, atlas) = rooms.connect(host("atlas"));
        let (_b, bay) = rooms.connect(host("bay"));
        let (_e, elm) = rooms.connect(host("elm"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;
        join(&bay, &directory, "ghp_bay", room.clone()).await;
        let projects = map();

        // A post refused before the store adds nothing.
        assert_eq!(
            elm.post_at(
                &projects,
                &room,
                MessageKind::Claim,
                body("forged"),
                Instant::now()
            )
            .await,
            Posted::NotMember
        );
        assert_eq!(rooms.ingest.len(), 0);

        // One entry while a post writes and another waits behind it.
        let writing = atlas.post_at(
            &projects,
            &room,
            MessageKind::Claim,
            body("a"),
            Instant::now(),
        );
        tokio::pin!(writing);
        until_writing(&store, 0, writing.as_mut()).await;
        assert_eq!(rooms.ingest.len(), 1);
        {
            let waiting = bay.post_at(
                &projects,
                &room,
                MessageKind::Claim,
                body("b"),
                Instant::now(),
            );
            tokio::pin!(waiting);
            assert!(
                poll_once(waiting.as_mut()).await.is_none(),
                "bay's post did not wait for atlas's"
            );
            assert_eq!(rooms.ingest.len(), 1);
            // Dropped while waiting.
        }
        assert_eq!(
            rooms.ingest.len(),
            1,
            "a dropped waiter took atlas's entry with it"
        );

        // Finishing lets go.
        let posted = writing.await;
        assert!(is_delivered(&posted), "{posted:?}");
        assert_eq!(
            rooms.ingest.len(),
            0,
            "a finished post left its entry behind"
        );

        // So does a post dropped mid-write, the way a cancelled post is.
        {
            let cancelled = bay.post_at(
                &projects,
                &room,
                MessageKind::Claim,
                body("c"),
                Instant::now(),
            );
            tokio::pin!(cancelled);
            until_writing(&store, 1, cancelled.as_mut()).await;
            assert_eq!(rooms.ingest.len(), 1);
        }
        assert_eq!(
            rooms.ingest.len(),
            0,
            "a cancelled post left its entry behind"
        );
    }

    #[tokio::test]
    async fn a_post_past_the_ingest_table_ceiling_is_refused_without_touching_the_store() {
        let store = Arc::new(Fake::new());
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_a, atlas) = rooms.connect(host("atlas"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;

        // Every slot taken by some other room.
        let mut held: Vec<IngestLease<'_>> = (1..=u32::try_from(MAX_INGEST_ROOMS).unwrap())
            .map(|n| rooms.ingest.lease(&ticket_room("example-repo", n)).unwrap())
            .collect();
        assert_eq!(rooms.ingest.len(), MAX_INGEST_ROOMS);

        assert_eq!(
            atlas
                .post_at(
                    &map(),
                    &room,
                    MessageKind::Claim,
                    body("no room"),
                    Instant::now()
                )
                .await,
            refused(&room, PostRefusal::NotRecorded)
        );
        assert_eq!(
            rooms.ingest.len(),
            MAX_INGEST_ROOMS,
            "the table grew past its ceiling"
        );
        assert_eq!(store.seq_reads(), 0, "a refused post read the store");
        assert_eq!(store.appends(), 0, "a refused post wrote to the store");

        // A slot given back is a post let through.
        held.pop();
        let posted = atlas
            .post_at(
                &map(),
                &room,
                MessageKind::Claim,
                body("room now"),
                Instant::now(),
            )
            .await;
        assert!(is_delivered(&posted), "{posted:?}");
    }

    #[tokio::test]
    async fn a_write_the_store_could_not_take_backs_the_room_off_like_a_failed_seed() {
        // XYZ-97's other half: the append path had no backoff, so in a
        // brownout every post to the room paid the whole persist budget with
        // its lock held, each behind the last.
        let store = Arc::new(Fake::failing());
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (mut atlas_inbox, atlas) = rooms.connect(host("atlas"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;

        let start = Instant::now();
        assert_eq!(
            atlas
                .post_at(&map(), &room, MessageKind::Claim, body("one"), start)
                .await,
            refused(&room, PostRefusal::NotRecorded)
        );
        assert_eq!(store.appends(), 1);

        // More than the post burst, since a refusal inside the window spends
        // no rate.
        for i in 1..=POST_BURST + 2 {
            assert_eq!(
                atlas
                    .post_at(
                        &map(),
                        &room,
                        MessageKind::Claim,
                        body("again"),
                        start + STORE_BACKOFF / 2
                    )
                    .await,
                refused(&room, PostRefusal::NotRecorded),
                "post {i} inside the backoff"
            );
        }
        assert_eq!(store.appends(), 1, "a post inside the backoff wrote again");

        assert_eq!(
            atlas
                .post_at(
                    &map(),
                    &room,
                    MessageKind::Claim,
                    body("later"),
                    start + STORE_BACKOFF
                )
                .await,
            refused(&room, PostRefusal::NotRecorded)
        );
        assert_eq!(store.appends(), 2, "the backoff window never expired");
        assert!(drain(&mut atlas_inbox).is_empty());
    }

    #[tokio::test]
    async fn a_room_backing_off_refuses_without_waiting_on_its_ingest_lock() {
        // The refusal inside a backoff window is decided before the lock, so
        // it is answered at once even while another post holds the room.
        let store = Arc::new(Fake::unreadable());
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Grants::new(&[("ghp_atlas", EXAMPLE_REPO)]);
        let room = repo_room("example-repo", EXAMPLE_REPO);
        let (_a, atlas) = rooms.connect(host("atlas"));
        join(&atlas, &directory, "ghp_atlas", room.clone()).await;

        let start = Instant::now();
        assert_eq!(
            atlas
                .post_at(&map(), &room, MessageKind::Claim, body("unseeded"), start)
                .await,
            refused(&room, PostRefusal::NotRecorded)
        );

        let held = rooms.ingest.lease(&room).unwrap().acquire().await;
        let answered = tokio::time::timeout(
            Duration::from_millis(500),
            atlas.post_at(
                &map(),
                &room,
                MessageKind::Claim,
                body("backing off"),
                start + STORE_BACKOFF / 2,
            ),
        )
        .await
        .expect("a post inside the backoff waited on the room's lock");
        assert_eq!(answered, refused(&room, PostRefusal::NotRecorded));
        drop(held);
        assert_eq!(store.seq_reads(), 1);
    }

    #[tokio::test]
    async fn a_post_to_an_archived_room_is_refused_before_the_store_is_read() {
        // XYZ-109 item 3: an unseeded archived room used to be seeded first,
        // spending a store read on a post refused whatever it found — and,
        // with the table down, answering `not_recorded` instead of `archived`.
        let store = Arc::new(Fake::new());
        let rooms = rooms_over(Arc::clone(&store));
        let directory = Merges::merged("feature/eng-12-portal");
        let room = ticket_room("example-repo", 12);
        let (_a, atlas) = rooms.connect(host("atlas"));
        atlas
            .join(&directory, &map(), &token("ghp_atlas"), room.clone())
            .await;

        assert_eq!(
            post_as(&atlas, &directory, "ghp_atlas", &room, "closed").await,
            refused(&room, PostRefusal::Archived)
        );
        assert!(rooms.is_archived(&room));
        assert_eq!(store.seq_reads(), 0, "an archived room was seeded");
        assert_eq!(store.appends(), 0);
        assert_eq!(rooms.ingest.len(), 0);
    }
}
