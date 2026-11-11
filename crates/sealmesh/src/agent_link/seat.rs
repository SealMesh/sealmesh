//! An agent's seat in the embedded router.
//!
//! Each configured agent holds rooms in the std daemon's own router, under its
//! own name, through the router's room registry directly: `Rooms::connect`
//! with the agent's name as the connection's identity. That is the same
//! registry, join check, post rate, token allowance, turn cap, body cap and
//! history every daemon connection goes through, so nothing here is a second
//! set of limits. What a seat does not do is open a WebSocket: it would show
//! up in the connection registry as a machine `who` asks, and it would need
//! the shared secret on a second socket for nothing the in-process handle
//! cannot already do.
//!
//! The name is stamped here and nowhere else. The link resolves a request's
//! token to an agent, picks that agent's seat, and the seat's membership
//! carries the name: the router's `from` on every message it posts, the label
//! a named room's member list is checked against, and the key its limits are
//! counted under. A request has no field that could name an author.
//!
//! One task per seat owns its membership and runs one command at a time, so
//! one agent's posts are ordered and its history reads never overlap, the
//! discipline a daemon connection keeps in `http::serve_daemon`. Messages the
//! router delivers to the seat go out on a broadcast channel that every open
//! stream of that agent subscribes to; a stream that falls behind it is
//! ended rather than buffered without bound, and the agent resumes with a
//! backfill.

use std::sync::Arc;

use sealmesh_chat_wire::{Body, MessageKind, RoomId, RoomName, Scope};
use sealmesh_proto::{RoomSeq, SecretToken, ServerFrame};
use sealmesh_server::{auth::Directory, http::App, rooms::Posted};
use sealmesh_wire::HostId;
use serde::Serialize;
use tokio::sync::{broadcast, mpsc, oneshot};

/// Commands one seat may have waiting. A full queue answers `busy` (429).
pub const SEAT_QUEUE: usize = 16;

/// Messages one seat buffers for its streams. A stream more than this far
/// behind is ended, and resumes from history.
pub const STREAM_BUFFER: usize = 256;

/// One room message as an agent sees it: on a stream, in a backfill.
///
/// `author` is the router's attribution, the name the poster's seat or
/// connection holds rooms under. Never anything a poster wrote about itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentMessage {
    pub room: RoomName,
    pub seq: RoomSeq,
    pub author: HostId,
    pub kind: MessageKind,
    pub body: Body,
}

#[derive(Debug)]
pub enum Command {
    Post {
        room: RoomName,
        kind: MessageKind,
        body: Body,
        reply: oneshot::Sender<Posted>,
    },
    Backfill {
        room: RoomName,
        after: Option<RoomSeq>,
        reply: oneshot::Sender<ServerFrame>,
    },
}

/// The link's handle on one seated agent.
#[derive(Debug, Clone)]
pub struct Seat {
    pub name: HostId,
    /// The rooms it may use: listed as a member, narrowed by its allowlist.
    pub rooms: Vec<RoomName>,
    commands: mpsc::Sender<Command>,
    messages: broadcast::Sender<Arc<AgentMessage>>,
}

/// Why a seat could not take a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeatBusy {
    /// Its queue is full.
    Full,
    /// Its task has ended (the daemon is stopping).
    Gone,
}

impl Seat {
    /// Seats `name` in `app`'s router and joins each of `rooms`. The task
    /// runs until every handle on the seat is dropped.
    ///
    /// `token` is the router's local secret: the daemon is its one member,
    /// and a seat is the daemon acting for one of its agents. It is handed to
    /// the router's own calls and nowhere else; it never decides a named
    /// room, which is checked against `name`.
    pub fn spawn<D: Directory + 'static>(
        app: Arc<App<D>>,
        token: SecretToken,
        name: HostId,
        rooms: Vec<RoomName>,
    ) -> Self {
        let (commands, queue) = mpsc::channel(SEAT_QUEUE);
        let (messages, _) = broadcast::channel(STREAM_BUFFER);
        tokio::spawn(run(
            app,
            token,
            name.clone(),
            rooms.clone(),
            queue,
            messages.clone(),
        ));
        Self {
            name,
            rooms,
            commands,
            messages,
        }
    }

    pub fn may_use(&self, room: &RoomName) -> bool {
        self.rooms.contains(room)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<AgentMessage>> {
        self.messages.subscribe()
    }

    pub async fn post(
        &self,
        room: RoomName,
        kind: MessageKind,
        body: Body,
    ) -> Result<Posted, SeatBusy> {
        let (reply, answer) = oneshot::channel();
        self.send(Command::Post {
            room,
            kind,
            body,
            reply,
        })?;
        answer.await.map_err(|_| SeatBusy::Gone)
    }

    pub async fn backfill(
        &self,
        room: RoomName,
        after: Option<RoomSeq>,
    ) -> Result<ServerFrame, SeatBusy> {
        let (reply, answer) = oneshot::channel();
        self.send(Command::Backfill { room, after, reply })?;
        answer.await.map_err(|_| SeatBusy::Gone)
    }

    fn send(&self, command: Command) -> Result<(), SeatBusy> {
        self.commands
            .try_send(command)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => SeatBusy::Full,
                mpsc::error::TrySendError::Closed(_) => SeatBusy::Gone,
            })
    }
}

async fn run<D: Directory>(
    app: Arc<App<D>>,
    token: SecretToken,
    name: HostId,
    rooms: Vec<RoomName>,
    mut queue: mpsc::Receiver<Command>,
    messages: broadcast::Sender<Arc<AgentMessage>>,
) {
    let (mut deliveries, membership) = app.rooms.connect(name.clone());
    for room in rooms {
        let frame = membership
            .join(
                &app.directory,
                &app.projects,
                &token,
                RoomId::named(room.clone()),
            )
            .await;
        if !matches!(frame, ServerFrame::Joined { .. }) {
            eprintln!("agent link: {name} could not join #{room}; its posts there will be refused");
        }
    }
    loop {
        tokio::select! {
            command = queue.recv() => {
                let Some(command) = command else { break };
                match command {
                    Command::Post { room, kind, body, reply } => {
                        let posted = membership
                            .post(
                                &app.directory,
                                &app.projects,
                                &token,
                                &RoomId::named(room),
                                kind,
                                body,
                            )
                            .await;
                        let _ = reply.send(posted);
                    }
                    Command::Backfill { room, after, reply } => {
                        let read = membership
                            .backfill(&app.projects, RoomId::named(room), after)
                            .await;
                        let _ = reply.send(read);
                    }
                }
            }
            delivered = membership.next_delivery(&mut deliveries) => {
                let Some(frame) = delivered else { break };
                if let ServerFrame::Message { room, seq, from, kind, body } = frame
                    && let Scope::Named(room) = room.scope
                {
                    // No subscriber is not an error: nobody is streaming now.
                    let _ = messages.send(Arc::new(AgentMessage {
                        room,
                        seq,
                        author: from,
                        kind,
                        body,
                    }));
                }
            }
        }
    }
}
