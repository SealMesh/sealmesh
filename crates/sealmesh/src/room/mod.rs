//! Room chat on the daemon: membership, an inbox, and a local socket.
//!
//! Room frames only work on a daemon connection, and the router keeps one
//! daemon connection per host, so `sealmesh chat`, the MCP room tools and
//! the hook cannot each open their own. The daemon owns membership and an
//! inbox, and local clients reach it over a Unix socket only this user can
//! open.
//!
//! Three pieces, split by who runs them:
//!
//! - [`client::RoomClient`] runs inside the daemon's session loop. It turns
//!   commands into frames and frames into inbox entries and replies. Every
//!   method returns at once: nothing in it waits on the router, on a local
//!   client, or on a full channel, so it can never delay the daemon's answer
//!   to a `Query`.
//! - [`server`] is a separate task. It accepts local connections, reads and
//!   writes them under deadlines, answers reads from the shared [`inbox`]
//!   itself, and hands anything that needs the router to the session over a
//!   bounded channel with `try_send`, so a full channel is a `busy` answer,
//!   never a wait.
//! - [`connect`] is the small client library the three local consumers share.
//!
//! Everything is gated on `Config::chat_enabled(daemon::is_off())`.

pub mod client;
pub mod connect;
pub mod inbox;
pub mod local;
pub mod resolve;
pub mod server;
pub mod socket;

use std::sync::{Arc, Mutex, MutexGuard};

use sealmesh_chat_wire::{Body, MessageKind, RoomId, Scope};
use sealmesh_proto::RoomSeq;
use sealmesh_wire::{RepoSlug, TicketId};
use tokio::sync::{mpsc, oneshot, watch};

/// A room's name for a human: `<project>/<ticket-or-repo>`, or the bare
/// project for its project-wide room. Shared by `sealmesh audit` and
/// `sealmesh chat`, which are the only two surfaces that print a `RoomId`
/// to a person rather than carry it on a protocol.
pub fn room_name(room: &RoomId) -> String {
    match &room.scope {
        Scope::Ticket(ticket) => format!("{}/{ticket}", room.project),
        Scope::Repo(repo) => format!("{}/{repo}", room.project),
        Scope::Project => room.project.to_string(),
    }
}

/// The word a closed, `snake_case` wire enum encodes as — `PostRefusal`,
/// `BackfillRefusal`, `MessageKind`, or a [`local::Category`] over one of
/// them — read back off its own `Serialize`, so this can never drift from
/// what the wire actually says. Falls back to `"unknown"` rather than
/// panicking on a type this was not written for (rule 39).
pub fn word<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

use self::{
    inbox::State,
    local::{ConsumerKey, Response},
};

/// How many router-bound requests may wait for the session at once. A local
/// client that finds it full is answered `busy`.
pub const COMMAND_QUEUE: usize = 32;

/// Whether chat is on right now: `[chat] enabled` and no kill file. Asked
/// again on every request and every room frame, so creating the kill file
/// takes effect without a restart.
pub type Gate = Arc<dyn Fn() -> bool + Send + Sync>;

/// The room state local clients read, shared between the session and the
/// socket server. The lock is held only for bounded, synchronous work and
/// never across an `.await`.
#[derive(Debug)]
pub struct Shared {
    state: Mutex<State>,
    arrivals: watch::Sender<u64>,
}

impl Shared {
    pub fn new(state: State) -> Arc<Self> {
        let (arrivals, _) = watch::channel(0);
        Arc::new(Self {
            state: Mutex::new(state),
            arrivals,
        })
    }

    /// The state. A panic while it was held cannot happen outside tests
    /// (rule 39), and the state is valid after any partial update anyway, so
    /// a poisoned lock is simply taken.
    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Wakes every tail waiting for something new. Never blocks.
    pub fn notify(&self) {
        self.arrivals.send_modify(|n| *n = n.wrapping_add(1));
    }

    pub fn arrivals(&self) -> watch::Receiver<u64> {
        self.arrivals.subscribe()
    }
}

/// A request that needs the router, on its way from the socket server to the
/// session.
#[derive(Debug)]
pub struct Command {
    pub op: Op,
    pub reply: oneshot::Sender<Response>,
}

#[derive(Debug)]
pub enum Op {
    /// `holder` is `None` for an anonymous/permanent claim (an MCP tool's
    /// `join`, or any caller that does not distinguish itself) and `Some` for
    /// a claim this daemon tracks by name — see [`local::Request::Join`].
    Join(RoomId, Option<ConsumerKey>),
    /// Join `room` under a lease rather than a permanent claim: what a
    /// `resolve` with `join` and a `hold` submit for each room. `holder` is
    /// `None` for a caller that names none. Renewed by asking again; dropped
    /// once idle past `client::LEASE_IDLE`, or when the router says the room
    /// is archived.
    Hold(RoomId, Option<ConsumerKey>),
    /// Removes one holder's claim and its lease: `None` the anonymous ones,
    /// `Some` that holder's. The room is left only once no claim remains —
    /// see [`local::Request::Leave`].
    Leave(RoomId, Option<ConsumerKey>),
    Post {
        room: RoomId,
        kind: MessageKind,
        body: Body,
        consumer: Option<ConsumerKey>,
    },
    Backfill {
        room: RoomId,
        after: Option<RoomSeq>,
    },
    List,
    /// A checkout's rooms, from the project map this connection was sent.
    /// Answered `Resolved`; joining them is the socket server's business,
    /// through `Join`.
    Resolve {
        repo: RepoSlug,
        ticket: Option<TicketId>,
    },
}

pub fn commands() -> (mpsc::Sender<Command>, mpsc::Receiver<Command>) {
    mpsc::channel(COMMAND_QUEUE)
}
