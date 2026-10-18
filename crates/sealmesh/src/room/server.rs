//! The local socket server: a task of its own, never the session loop.
//!
//! It accepts a connection only from this user (peer uid, checked on
//! accept), holds at most `Limits::connections` at once, reads one request
//! under a deadline and a byte cap, and writes one response under a deadline.
//! Reads of the inbox are answered here from the shared state. A request that
//! needs the router is handed to the session with `try_send` on a bounded
//! channel — a full channel is answered `busy` — and its answer is awaited
//! here, under a deadline, never in the session. So a local client that
//! stalls, floods, or never reads its answer costs this task a slot, and the
//! session nothing.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::{
    io::{AsyncBufReadExt as _, AsyncRead, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    net::{UnixListener, UnixStream},
    sync::{Semaphore, mpsc, oneshot},
};

use super::{
    Command, Gate, Op, Shared,
    inbox::{DrainTicket, Peeked},
    local::{
        self, JoinOutcome, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, MAX_TAIL_WAIT_MS, Request,
        Response,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Local connections served at once. One more is closed on accept.
    pub connections: usize,
    /// How long a client has to send its whole request.
    pub read_deadline: Duration,
    /// How long a client has to take its whole response.
    pub write_deadline: Duration,
    /// How long a request that needs the router waits for the session's
    /// answer. Longer than the session's own deadline for the router, so the
    /// session's `timed_out` normally arrives first.
    pub answer_deadline: Duration,
    /// The longest a tail may wait for something to arrive, whatever it asks.
    pub max_tail_wait: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            connections: 16,
            read_deadline: Duration::from_secs(5),
            write_deadline: Duration::from_secs(5),
            answer_deadline: super::client::Timing::default().router_wait + Duration::from_secs(2),
            max_tail_wait: Duration::from_millis(u64::from(MAX_TAIL_WAIT_MS)),
        }
    }
}

impl Limits {
    /// The longest this server can take to answer one request: reading it,
    /// waiting on the session or on a tail, then writing. A client deadline
    /// below this can report `timed_out` on a request still being answered.
    pub fn longest_answer(&self) -> Duration {
        self.read_deadline
            .saturating_add(self.answer_deadline.max(self.max_tail_wait))
            .saturating_add(self.write_deadline)
    }
}

/// Counters a test can watch, so "the stall is happening" is observed rather
/// than assumed.
#[derive(Debug, Default)]
pub struct Stats {
    pub accepted: AtomicU64,
    pub refused_peer: AtomicU64,
    pub refused_full: AtomicU64,
    pub busy: AtomicU64,
    pub reading: AtomicU64,
    pub awaiting_router: AtomicU64,
    /// Drains whose answer was not written, so their cursors did not move.
    pub unwritten_drains: AtomicU64,
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::SeqCst);
}

fn drop_one(counter: &AtomicU64) {
    counter.fetch_sub(1, Ordering::SeqCst);
}

pub struct Server {
    pub shared: Arc<Shared>,
    pub commands: mpsc::Sender<Command>,
    pub gate: Gate,
    /// The only uid a peer may have: the one that owns the socket.
    pub uid: u32,
    pub limits: Limits,
    pub stats: Arc<Stats>,
}

/// Accepts local connections until the listener fails for good.
pub async fn serve(listener: UnixListener, server: Arc<Server>) {
    let slots = Arc::new(Semaphore::new(server.limits.connections));
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(error) => {
                // Out of descriptors, most likely. Back off rather than spin.
                eprintln!("local socket accept failed: {error}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        if !peer_allowed(&stream, server.uid) {
            bump(&server.stats.refused_peer);
            eprintln!("refused a local connection not confirmed to be from this user");
            continue;
        }
        let Ok(slot) = slots.clone().try_acquire_owned() else {
            bump(&server.stats.refused_full);
            continue;
        };
        bump(&server.stats.accepted);
        let server = server.clone();
        tokio::spawn(async move {
            handle(stream, &server).await;
            drop(slot);
        });
    }
}

/// Whether the process on the other end runs as `uid`. A peer whose
/// credentials cannot be read is refused.
pub fn peer_allowed(stream: &UnixStream, uid: u32) -> bool {
    stream.peer_cred().is_ok_and(|cred| cred.uid() == uid)
}

enum Read {
    Line(Vec<u8>),
    TooLarge,
}

/// Reads one line of at most `limit` bytes, newline included. Stops reading
/// at the cap, so an endless line costs at most `limit + 1` bytes of memory.
pub(crate) async fn read_line<R: AsyncRead + Unpin>(
    mut reader: R,
    limit: usize,
) -> std::io::Result<Option<Vec<u8>>> {
    match read_capped(&mut reader, limit).await? {
        Read::Line(line) => Ok(Some(line)),
        Read::TooLarge => Ok(None),
    }
}

async fn read_capped<R: AsyncRead + Unpin>(reader: &mut R, limit: usize) -> std::io::Result<Read> {
    let cap = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
    let mut reader = BufReader::new(reader.take(cap));
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line).await?;
    if line.len() > limit {
        return Ok(Read::TooLarge);
    }
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    Ok(Read::Line(line))
}

/// The most a connection's leftover input is read and thrown away before it
/// is closed: a few requests' worth.
pub const MAX_DRAIN_BYTES: usize = 4 * MAX_REQUEST_BYTES;

/// Reads and discards what `reader` still holds, into a fixed buffer, until
/// the peer closes, `MAX_DRAIN_BYTES` have gone, or `limit` passes.
///
/// Closing a Unix socket with unread input makes Linux reset the connection,
/// and the reset discards an answer already written but not yet read. So a
/// refused oversized request would reach a Linux client as a bare reset
/// rather than `invalid`. Draining first lets the answer arrive; the cap and
/// the deadline keep a client that never stops sending from holding the slot.
async fn drain<R: AsyncRead + Unpin>(reader: &mut R, limit: Duration) {
    let mut buffer = [0u8; 8192];
    let mut left = MAX_DRAIN_BYTES;
    let _ = tokio::time::timeout(limit, async {
        while left > 0 {
            match reader.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(n) => left = left.saturating_sub(n),
            }
        }
    })
    .await;
}

async fn handle(stream: UnixStream, server: &Server) {
    let (mut read, mut write) = stream.into_split();

    bump(&server.stats.reading);
    let request = tokio::time::timeout(
        server.limits.read_deadline,
        read_capped(&mut read, MAX_REQUEST_BYTES),
    )
    .await;
    drop_one(&server.stats.reading);

    let (response, pending) = match request {
        // A client that never finishes its request gets nothing back. Nothing
        // is written, so there is no answer for a reset to discard.
        Err(_) | Ok(Err(_)) => return,
        Ok(Ok(Read::TooLarge)) => (Response::Invalid {}, None),
        Ok(Ok(Read::Line(bytes))) => match local::decode::<Request>(&bytes, MAX_REQUEST_BYTES) {
            Ok(request) => respond(request, server).await,
            Err(_) => (Response::Invalid {}, None),
        },
    };

    // A drain's messages that do not fit are answered `too_large`, which
    // hands nothing out, so `pending` is let go of rather than committed.
    let (line, pending) = match local::encode(&response, MAX_RESPONSE_BYTES) {
        Ok(line) => (Ok(line), pending),
        Err(_) => (
            local::encode(&Response::TooLarge {}, MAX_RESPONSE_BYTES),
            None,
        ),
    };
    let Ok(line) = line else { return };
    let written = tokio::time::timeout(server.limits.write_deadline, async {
        write.write_all(&line).await?;
        write.shutdown().await
    })
    .await;
    let written = matches!(written, Ok(Ok(())));
    // A drain moves its consumer's cursors only once its answer is out. Not
    // written — the client gave up, the write failed or ran out of time, or
    // this task was cancelled — and `pending` is dropped, which lets go of it
    // and leaves the cursors where they were.
    if let Some(pending) = pending {
        if written {
            pending.commit();
        } else {
            bump(&server.stats.unwritten_drains);
        }
    }
    // Every answer, `invalid`, `busy` and `disabled` included, goes out
    // before the socket closes. Anything the client sent past the request
    // line — the rest of an oversized request, or trailing bytes — is drained
    // first, so closing does not reset the connection under the answer.
    if written {
        drain(&mut read, server.limits.read_deadline).await;
    }
}

/// A drain read from the inbox whose answer is not yet written. Committing
/// moves its consumer's cursors; dropping it any other way — including this
/// task being cancelled mid-write — lets go of it and moves nothing, so a
/// drain is never counted as handed out unless it was.
struct PendingDrain<'a> {
    shared: &'a Shared,
    ticket: Option<DrainTicket>,
}

impl PendingDrain<'_> {
    fn commit(mut self) {
        if let Some(ticket) = self.ticket.take() {
            self.shared.lock().commit_drain(ticket);
        }
    }
}

impl Drop for PendingDrain<'_> {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            self.shared.lock().release_drain(&ticket);
        }
    }
}

/// The answer to `request`, and for a drain, what commits it once written.
async fn respond(request: Request, server: &Server) -> (Response, Option<PendingDrain<'_>>) {
    if !(server.gate)() {
        return (Response::Disabled {}, None);
    }
    let op = match request {
        Request::Status {} => {
            return (
                Response::Status {
                    status: server.shared.lock().status(),
                },
                None,
            );
        }
        Request::Drain {
            consumer,
            room,
            max_messages,
            max_bytes,
        } => {
            let peeked = server.shared.lock().peek_drain(
                &consumer,
                room.as_ref(),
                max_messages,
                max_bytes,
                jiff::Timestamp::now(),
            );
            let Peeked::Ready(drained, ticket) = peeked else {
                // This consumer's previous drain is still being written.
                // Answering now would hand out the same messages twice.
                bump(&server.stats.busy);
                return (Response::Busy {}, None);
            };
            let pending = PendingDrain {
                shared: &server.shared,
                ticket: Some(ticket),
            };
            return (
                Response::Messages {
                    messages: drained.messages,
                    lost: drained.lost,
                    new_consumer: drained.new_consumer,
                    more: drained.more,
                },
                Some(pending),
            );
        }
        Request::Tail {
            room,
            after,
            wait_ms,
        } => return (tail(server, &room, after, wait_ms).await, None),
        Request::Join { room, holder } => Op::Join(room, holder),
        Request::Leave { room, holder } => Op::Leave(room, holder),
        Request::Post {
            room,
            kind,
            body,
            consumer,
        } => Op::Post {
            room,
            kind,
            body,
            consumer,
        },
        Request::Backfill { room, after } => Op::Backfill { room, after },
        Request::ListRooms {} => Op::List,
        Request::Resolve { repo, ticket, join } => {
            // An older client's `resolve` names no holder: its rooms are held
            // under the lease of a caller that names none.
            let lease = join.then_some(None);
            return (
                resolve(server, Op::Resolve { repo, ticket }, lease).await,
                None,
            );
        }
        Request::Hold {
            repo,
            ticket,
            holder,
        } => {
            return (
                resolve(server, Op::Resolve { repo, ticket }, Some(Some(holder))).await,
                None,
            );
        }
    };
    let deadline = tokio::time::Instant::now() + server.limits.answer_deadline;
    (ask_session(server, op, deadline).await, None)
}

/// A checkout's rooms, and with `lease`, each joined in turn through the
/// session's ordinary join path — the same one a `join` request takes, so the
/// router decides every one. The whole request, resolution and joins, is
/// bounded by one `answer_deadline`, so it cannot outlast any other request.
///
/// `lease` is `None` for a resolution that joins nothing, and otherwise the
/// holder whose lease each room is held under (`Some(None)`: one that names
/// no holder).
async fn resolve(server: &Server, op: Op, lease: Option<Option<local::ConsumerKey>>) -> Response {
    let deadline = tokio::time::Instant::now() + server.limits.answer_deadline;
    let resolved = ask_session(server, op, deadline).await;
    let Response::Resolved { rooms } = resolved else {
        return resolved;
    };
    let Some(holder) = lease else {
        return Response::Resolved { rooms };
    };
    // Handed to the session together, where joins queue one behind another,
    // then awaited against what is left of the one deadline.
    // A lease, not a permanent claim: the hook resolves on every prompt, so
    // a room it stops naming — a branch left, a session ended — is let go
    // once idle rather than held against the room cap for good.
    let submitted: Vec<Result<oneshot::Receiver<Response>, Response>> = rooms
        .iter()
        .map(|resolved| submit(server, Op::Hold(resolved.room.clone(), holder.clone())))
        .collect();
    let mut joined = Vec::with_capacity(rooms.len());
    for (mut resolved, submission) in rooms.into_iter().zip(submitted) {
        let response = match submission {
            Ok(answer) => await_answer(server, answer, deadline).await,
            Err(refused) => refused,
        };
        if let Response::Joined { room } = &response {
            resolved.room = room.clone();
        }
        resolved.join = Some(JoinOutcome::of(&response));
        joined.push(resolved);
    }
    Response::Resolved { rooms: joined }
}

async fn ask_session(server: &Server, op: Op, deadline: tokio::time::Instant) -> Response {
    match submit(server, op) {
        Ok(answer) => await_answer(server, answer, deadline).await,
        Err(response) => response,
    }
}

/// Hands `op` to the session without waiting: a full queue is `busy` at once.
fn submit(server: &Server, op: Op) -> Result<oneshot::Receiver<Response>, Response> {
    if !server.shared.lock().connected() {
        return Err(Response::NotConnected {});
    }
    let (reply, answer) = oneshot::channel();
    match server.commands.try_send(Command { op, reply }) {
        Ok(()) => Ok(answer),
        Err(mpsc::error::TrySendError::Full(_)) => {
            bump(&server.stats.busy);
            Err(Response::Busy {})
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(Response::NotConnected {}),
    }
}

async fn await_answer(
    server: &Server,
    answer: oneshot::Receiver<Response>,
    deadline: tokio::time::Instant,
) -> Response {
    bump(&server.stats.awaiting_router);
    let answered = tokio::time::timeout_at(deadline, answer).await;
    drop_one(&server.stats.awaiting_router);
    match answered {
        Ok(Ok(response)) => response,
        Ok(Err(_)) => Response::NotConnected {},
        Err(_) => Response::TimedOut {},
    }
}

async fn tail(
    server: &Server,
    room: &sealmesh_chat_wire::RoomId,
    after: Option<local::TailCursor>,
    wait_ms: Option<u32>,
) -> Response {
    let asked = Duration::from_millis(u64::from(wait_ms.unwrap_or(0)));
    let wait = asked.min(server.limits.max_tail_wait);
    let deadline = tokio::time::Instant::now() + wait;
    // Subscribed before the first look, so an arrival between the look and
    // the wait is not missed.
    let mut arrivals = server.shared.arrivals();
    loop {
        let Some(tailed) = server.shared.lock().tail(room, after) else {
            return Response::NotMember { room: room.clone() };
        };
        let ready = !tailed.messages.is_empty() || tailed.lost > 0 || tailed.reset;
        if ready || tokio::time::Instant::now() >= deadline {
            return Response::Tail {
                room: tailed.room,
                messages: tailed.messages,
                cursor: tailed.cursor,
                lost: tailed.lost,
                reset: tailed.reset,
            };
        }
        if tokio::time::timeout_at(deadline, arrivals.changed())
            .await
            .is_ok_and(|changed| changed.is_err())
        {
            // The daemon is shutting down; answer with what there is.
            return Response::Tail {
                room: tailed.room,
                messages: tailed.messages,
                cursor: tailed.cursor,
                lost: tailed.lost,
                reset: tailed.reset,
            };
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

    use std::{path::PathBuf, sync::atomic::AtomicBool, time::Instant};

    use jiff::Timestamp;
    use sealmesh_chat_wire::{Body, MessageKind, RoomId, Scope};
    use sealmesh_proto::RoomSeq;
    use sealmesh_wire::{HostId, ProjectId, TicketId};

    use super::*;
    use crate::room::{
        connect,
        inbox::{Limits as InboxLimits, State},
        local::{ConsumerKey, Delivered},
        socket,
    };

    struct Rig {
        path: PathBuf,
        root: PathBuf,
        shared: Arc<Shared>,
        commands: mpsc::Receiver<Command>,
        stats: Arc<Stats>,
        on: Arc<AtomicBool>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for Rig {
        fn drop(&mut self) {
            self.task.abort();
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn start(tag: &str, limits: Limits, uid_offset: u32) -> Rig {
        let root = std::env::temp_dir().join(format!("aws{}{tag}", std::process::id() % 100_000));
        let _ = std::fs::remove_dir_all(&root);
        let path = root.join("local").join("daemon.sock");
        let (listener, uid) = socket::bind(&path).unwrap();
        listener.set_nonblocking(true).unwrap();
        let listener = UnixListener::from_std(listener).unwrap();
        let shared = Shared::new(State::new(InboxLimits::default(), Timestamp::now()));
        let (tx, commands) = mpsc::channel(4);
        let stats = Arc::new(Stats::default());
        let on = Arc::new(AtomicBool::new(true));
        let gate_on = on.clone();
        let server = Arc::new(Server {
            shared: shared.clone(),
            commands: tx,
            gate: Arc::new(move || gate_on.load(Ordering::SeqCst)),
            uid: uid.wrapping_add(uid_offset),
            limits,
            stats: stats.clone(),
        });
        let task = tokio::spawn(serve(listener, server));
        Rig {
            path,
            root,
            shared,
            commands,
            stats,
            on,
            task,
        }
    }

    fn ticket(n: u32) -> RoomId {
        RoomId::new(
            ProjectId::try_from("example-repo").unwrap(),
            Scope::Ticket(TicketId::new("ENG", n).unwrap()),
        )
    }

    async fn ask(rig: &Rig, request: &Request) -> Response {
        connect::request(&rig.path, request, Duration::from_secs(5))
            .await
            .unwrap()
    }

    async fn wait_for(counter: &AtomicU64, at_least: u64) {
        let started = Instant::now();
        while counter.load(Ordering::SeqCst) < at_least {
            assert!(started.elapsed() < Duration::from_secs(5), "never happened");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn disabled_answers_at_once_and_never_reaches_the_session() {
        let mut rig = start("d", Limits::default(), 0);
        rig.on.store(false, Ordering::SeqCst);
        for request in [
            Request::Status {},
            Request::Join {
                room: ticket(1),
                holder: None,
            },
            Request::ListRooms {},
            Request::Resolve {
                repo: sealmesh_wire::RepoSlug::try_from("example-org/example-repo").unwrap(),
                ticket: None,
                join: true,
            },
            Request::Drain {
                consumer: ConsumerKey::new("a").unwrap(),
                room: None,
                max_messages: None,
                max_bytes: None,
            },
        ] {
            let started = Instant::now();
            assert_eq!(ask(&rig, &request).await, Response::Disabled {});
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "disabled took {:?}",
                started.elapsed()
            );
        }
        assert!(
            rig.commands.try_recv().is_err(),
            "a disabled request reached the session"
        );
    }

    #[tokio::test]
    async fn a_peer_with_another_uid_is_refused_on_accept() {
        // The server believes the socket belongs to a different uid, so this
        // process is the "other user".
        let rig = start("u", Limits::default(), 1);
        let result = connect::request(&rig.path, &Request::Status {}, Duration::from_secs(2)).await;
        assert!(
            matches!(result, Err(connect::ConnectError::Refused)),
            "{result:?}"
        );
        assert_eq!(rig.stats.refused_peer.load(Ordering::SeqCst), 1);
        assert_eq!(rig.stats.accepted.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_oversized_or_unknown_request_is_answered_invalid_without_an_echo() {
        let rig = start("i", Limits::default(), 0);
        for raw in [
            vec![b'x'; MAX_REQUEST_BYTES + 10],
            br#"{"op":"exec","cmd":"ghp_secretvalue"}"#.to_vec(),
            b"not json\n".to_vec(),
        ] {
            let mut stream = UnixStream::connect(&rig.path).await.unwrap();
            // The server may stop reading at the cap and answer; a write error
            // past that point is expected.
            let _ = stream.write_all(&raw).await;
            let _ = stream.write_all(b"\n").await;
            let mut answer = Vec::new();
            stream.read_to_end(&mut answer).await.unwrap();
            let text = String::from_utf8(answer).unwrap();
            assert_eq!(text, "{\"result\":\"invalid\"}\n");
        }
    }

    #[tokio::test]
    async fn a_client_that_never_finishes_its_request_is_dropped_at_the_deadline() {
        let limits = Limits {
            read_deadline: Duration::from_millis(200),
            ..Limits::default()
        };
        let rig = start("r", limits, 0);
        let mut stream = UnixStream::connect(&rig.path).await.unwrap();
        stream.write_all(br#"{"op":"sta"#).await.unwrap();
        let started = Instant::now();
        let mut answer = Vec::new();
        stream.read_to_end(&mut answer).await.unwrap();
        assert!(answer.is_empty());
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    #[tokio::test]
    async fn connections_past_the_cap_are_closed_and_counted() {
        let limits = Limits {
            connections: 2,
            read_deadline: Duration::from_secs(30),
            ..Limits::default()
        };
        let rig = start("c", limits, 0);
        let _a = UnixStream::connect(&rig.path).await.unwrap();
        let _b = UnixStream::connect(&rig.path).await.unwrap();
        wait_for(&rig.stats.reading, 2).await;
        let mut c = UnixStream::connect(&rig.path).await.unwrap();
        let mut answer = Vec::new();
        let _ = c.read_to_end(&mut answer).await;
        assert!(answer.is_empty());
        wait_for(&rig.stats.refused_full, 1).await;
        assert_eq!(rig.stats.accepted.load(Ordering::SeqCst), 2);
        // Through the client library, a refusal at the cap says so rather
        // than reading as a broken answer.
        let result = connect::request(&rig.path, &Request::Status {}, Duration::from_secs(2)).await;
        assert!(
            matches!(result, Err(connect::ConnectError::Refused)),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn bytes_after_the_request_line_do_not_cost_the_answer() {
        let rig = start("t2", Limits::default(), 0);
        let mut stream = UnixStream::connect(&rig.path).await.unwrap();
        stream
            .write_all(b"{\"op\":\"status\"}\ntrailing bytes nobody asked for")
            .await
            .unwrap();
        let mut answer = Vec::new();
        stream.read_to_end(&mut answer).await.unwrap();
        let text = String::from_utf8(answer).unwrap();
        assert!(text.starts_with("{\"result\":\"status\""), "{text}");
    }

    #[tokio::test]
    async fn a_full_command_queue_is_busy_not_a_wait() {
        let limits = Limits {
            answer_deadline: Duration::from_secs(30),
            connections: 16,
            ..Limits::default()
        };
        let rig = start("b", limits, 0);
        rig.shared
            .lock()
            .set_connected(Some(HostId::try_from("sandbank").unwrap()));
        // Nobody drains the queue of four: four requests wait, the fifth is
        // answered busy at once.
        let mut waiting = Vec::new();
        for _ in 0..4 {
            let path = rig.path.clone();
            waiting.push(tokio::spawn(async move {
                connect::request(&path, &Request::ListRooms {}, Duration::from_secs(60)).await
            }));
        }
        wait_for(&rig.stats.awaiting_router, 4).await;
        let started = Instant::now();
        assert_eq!(ask(&rig, &Request::ListRooms {}).await, Response::Busy {});
        assert!(started.elapsed() < Duration::from_secs(1));
        for task in waiting {
            task.abort();
        }
    }

    #[tokio::test]
    async fn a_router_request_while_disconnected_is_answered_at_once() {
        let rig = start("n", Limits::default(), 0);
        let started = Instant::now();
        assert_eq!(
            ask(
                &rig,
                &Request::Join {
                    room: ticket(1),
                    holder: None
                }
            )
            .await,
            Response::NotConnected {}
        );
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn a_request_the_session_never_answers_times_out() {
        let limits = Limits {
            answer_deadline: Duration::from_millis(200),
            ..Limits::default()
        };
        let rig = start("t", limits, 0);
        rig.shared
            .lock()
            .set_connected(Some(HostId::try_from("sandbank").unwrap()));
        assert_eq!(
            ask(&rig, &Request::ListRooms {}).await,
            Response::TimedOut {}
        );
    }

    #[tokio::test]
    async fn status_drain_and_tail_are_answered_from_the_inbox() {
        let rig = start("s", Limits::default(), 0);
        {
            let mut state = rig.shared.lock();
            state.set_connected(Some(HostId::try_from("sandbank").unwrap()));
            state.admit(ticket(1));
        }
        let consumer = ConsumerKey::new("session-a").unwrap();
        let drain = Request::Drain {
            consumer: consumer.clone(),
            room: None,
            max_messages: None,
            max_bytes: None,
        };
        let Response::Messages { new_consumer, .. } = ask(&rig, &drain).await else {
            panic!()
        };
        assert!(new_consumer);

        let tail_request = Request::Tail {
            room: ticket(1),
            after: None,
            wait_ms: Some(5_000),
        };
        let path = rig.path.clone();
        let tailing = tokio::spawn(async move {
            connect::request(&path, &tail_request, Duration::from_secs(10)).await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        rig.shared.lock().push(
            Delivered {
                room: ticket(1),
                seq: RoomSeq(1),
                from: HostId::try_from("shelter").unwrap(),
                kind: MessageKind::Claim,
                body: Body::new("hello").unwrap(),
                received_at: Timestamp::now(),
            },
            None,
        );
        rig.shared.notify();
        let started = Instant::now();
        let Response::Tail { messages, .. } = tailing.await.unwrap().unwrap() else {
            panic!()
        };
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the tail was not woken"
        );
        assert_eq!(messages.len(), 1);

        let Response::Messages { messages, .. } = ask(&rig, &drain).await else {
            panic!()
        };
        assert_eq!(messages.len(), 1);
        let Response::Messages { messages, .. } = ask(&rig, &drain).await else {
            panic!()
        };
        assert!(
            messages.is_empty(),
            "a message was handed to one consumer twice"
        );

        let Response::Status { status } = ask(&rig, &Request::Status {}).await else {
            panic!()
        };
        assert_eq!(status.rooms.len(), 1);
        assert!(status.connected);
    }

    fn push_messages(rig: &Rig, room: &RoomId, count: u64, body: &str) {
        for n in 1..=count {
            rig.shared.lock().push(
                Delivered {
                    room: room.clone(),
                    seq: RoomSeq(n),
                    from: HostId::try_from("shelter").unwrap(),
                    kind: MessageKind::Claim,
                    body: Body::new(body).unwrap(),
                    received_at: Timestamp::now(),
                },
                None,
            );
        }
    }

    fn seqs(response: &Response) -> Vec<RoomSeq> {
        let Response::Messages { messages, .. } = response else {
            panic!("{response:?}")
        };
        messages.iter().map(|m| m.seq).collect()
    }

    #[tokio::test]
    async fn a_drain_whose_client_is_gone_before_the_write_moves_nothing() {
        let rig = start("dg", Limits::default(), 0);
        rig.shared.lock().admit(ticket(1));
        push_messages(&rig, &ticket(1), 3, "hello");
        let drain = Request::Drain {
            consumer: ConsumerKey::new("session-a").unwrap(),
            room: None,
            max_messages: None,
            max_bytes: None,
        };

        // The client is accepted and being read, then finishes its drain and
        // is gone before the server can answer. Nothing yields between the
        // last byte and the close, so the server reads a whole request from
        // a peer that has already hung up.
        let line = local::encode(&drain, MAX_REQUEST_BYTES).unwrap();
        let (head, rest) = line.split_at(4);
        let mut gone = UnixStream::connect(&rig.path).await.unwrap();
        gone.write_all(head).await.unwrap();
        wait_for(&rig.stats.reading, 1).await;
        gone.write_all(rest).await.unwrap();
        drop(gone);
        wait_for(&rig.stats.unwritten_drains, 1).await;

        // Nothing was handed out: the next drain gets all three, still as a
        // new consumer, and the one after that gets none of them again.
        let answer = ask(&rig, &drain).await;
        assert_eq!(seqs(&answer), [RoomSeq(1), RoomSeq(2), RoomSeq(3)]);
        assert!(matches!(
            answer,
            Response::Messages {
                new_consumer: true,
                lost: 0,
                more: false,
                ..
            }
        ));
        assert!(seqs(&ask(&rig, &drain).await).is_empty());
        assert_eq!(rig.stats.unwritten_drains.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_drain_whose_write_runs_out_of_time_moves_nothing() {
        let limits = Limits {
            write_deadline: Duration::from_millis(200),
            ..Limits::default()
        };
        let rig = start("dt", limits, 0);
        rig.shared.lock().admit(ticket(1));
        // Far more than a socket buffer holds, so a client that never reads
        // stalls the write until its deadline.
        let big = "\u{1}".repeat(sealmesh_chat_wire::MAX_BODY_CHARS);
        push_messages(&rig, &ticket(1), 64, &big);
        let drain = Request::Drain {
            consumer: ConsumerKey::new("session-a").unwrap(),
            room: None,
            max_messages: None,
            max_bytes: None,
        };
        let mut stalled = UnixStream::connect(&rig.path).await.unwrap();
        stalled
            .write_all(&local::encode(&drain, MAX_REQUEST_BYTES).unwrap())
            .await
            .unwrap();
        wait_for(&rig.stats.unwritten_drains, 1).await;
        drop(stalled);

        // The room's byte cap has dropped the earliest; the drain that timed
        // out handed out none of what is left.
        let oldest = rig.shared.lock().tail(&ticket(1), None).unwrap().messages[0].seq;
        let answer = ask(&rig, &drain).await;
        assert_eq!(seqs(&answer).first(), Some(&oldest));
    }

    #[tokio::test]
    async fn written_drains_hand_each_message_out_exactly_once() {
        let rig = start("dx", Limits::default(), 0);
        rig.shared.lock().admit(ticket(1));
        push_messages(&rig, &ticket(1), 5, "hello");
        let mut seen = Vec::new();
        for _ in 0..5 {
            let answer = ask(
                &rig,
                &Request::Drain {
                    consumer: ConsumerKey::new("session-a").unwrap(),
                    room: None,
                    max_messages: Some(2),
                    max_bytes: None,
                },
            )
            .await;
            let more = matches!(answer, Response::Messages { more: true, .. });
            seen.extend(seqs(&answer));
            if !more {
                break;
            }
        }
        assert_eq!(seen, (1..=5).map(RoomSeq).collect::<Vec<_>>());
        assert_eq!(rig.stats.unwritten_drains.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_second_drain_while_the_first_is_being_written_is_busy() {
        let limits = Limits {
            write_deadline: Duration::from_secs(2),
            ..Limits::default()
        };
        let rig = start("do", limits, 0);
        rig.shared.lock().admit(ticket(1));
        // Four-byte characters, so 64 bodies pass the drain budget.
        let big = "😀".repeat(sealmesh_chat_wire::MAX_BODY_CHARS);
        push_messages(&rig, &ticket(1), 64, &big);
        let drain = Request::Drain {
            consumer: ConsumerKey::new("session-a").unwrap(),
            room: None,
            max_messages: None,
            max_bytes: None,
        };
        // The first answer is stuck behind a client that is not reading yet.
        let mut slow = UnixStream::connect(&rig.path).await.unwrap();
        slow.write_all(&local::encode(&drain, MAX_REQUEST_BYTES).unwrap())
            .await
            .unwrap();
        wait_for(&rig.stats.accepted, 1).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(ask(&rig, &drain).await, Response::Busy {});
        assert_eq!(rig.stats.busy.load(Ordering::SeqCst), 1);

        // Another consumer is not held up by any of it.
        let other = ask(
            &rig,
            &Request::Drain {
                consumer: ConsumerKey::new("session-b").unwrap(),
                room: None,
                max_messages: None,
                max_bytes: None,
            },
        )
        .await;
        assert!(!seqs(&other).is_empty());

        // Once the slow client reads, its answer is whole and committed: the
        // next drain carries on after it rather than repeating it.
        let mut answer = Vec::new();
        slow.read_to_end(&mut answer).await.unwrap();
        let first = seqs(&local::decode(&answer, MAX_RESPONSE_BYTES).unwrap());
        assert!(!first.is_empty());
        let started = Instant::now();
        let next = loop {
            let answer = ask(&rig, &drain).await;
            if answer != (Response::Busy {}) {
                break answer;
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "never committed"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let next = seqs(&next);
        assert!(
            next.iter().all(|seq| !first.contains(seq)),
            "{first:?} then {next:?}"
        );
        assert_eq!(next.first().map(|s| s.0), first.last().map(|s| s.0 + 1));
    }

    #[tokio::test]
    async fn a_tail_of_a_room_not_held_is_not_member() {
        let rig = start("m", Limits::default(), 0);
        assert_eq!(
            ask(
                &rig,
                &Request::Tail {
                    room: ticket(1),
                    after: None,
                    wait_ms: None
                }
            )
            .await,
            Response::NotMember { room: ticket(1) }
        );
    }

    #[tokio::test]
    async fn a_tail_asking_to_wait_longer_than_the_cap_waits_only_the_cap() {
        let limits = Limits {
            max_tail_wait: Duration::from_millis(200),
            ..Limits::default()
        };
        let rig = start("w", limits, 0);
        rig.shared.lock().admit(ticket(1));
        let started = Instant::now();
        let answer = connect::request(
            &rig.path,
            &Request::Tail {
                room: ticket(1),
                after: None,
                wait_ms: Some(u32::MAX),
            },
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        assert!(matches!(answer, Response::Tail { .. }), "{answer:?}");
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "waited {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn a_client_that_never_reads_its_answer_gives_its_slot_back() {
        // One slot. A client asks for a large answer and never reads it; once
        // the write deadline passes, the slot is free for the next client.
        let limits = Limits {
            connections: 1,
            write_deadline: Duration::from_millis(200),
            ..Limits::default()
        };
        let rig = start("v", limits, 0);
        rig.shared.lock().admit(ticket(1));
        let big = "\u{1}".repeat(sealmesh_chat_wire::MAX_BODY_CHARS);
        for n in 1..=64 {
            rig.shared.lock().push(
                Delivered {
                    room: ticket(1),
                    seq: RoomSeq(n),
                    from: HostId::try_from("shelter").unwrap(),
                    kind: MessageKind::Claim,
                    body: Body::new(big.clone()).unwrap(),
                    received_at: Timestamp::now(),
                },
                None,
            );
        }
        let mut stalled = UnixStream::connect(&rig.path).await.unwrap();
        let request = local::encode(
            &Request::Tail {
                room: ticket(1),
                after: None,
                wait_ms: None,
            },
            MAX_REQUEST_BYTES,
        )
        .unwrap();
        stalled.write_all(&request).await.unwrap();
        wait_for(&rig.stats.accepted, 1).await;

        let started = Instant::now();
        loop {
            // Refused while the stalled write holds the only slot.
            if let Ok(Response::Status { .. }) =
                connect::request(&rig.path, &Request::Status {}, Duration::from_secs(1)).await
            {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the stalled writer never gave its slot back"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        drop(stalled);
    }

    #[tokio::test]
    async fn a_resolve_and_its_joins_share_one_deadline() {
        const DEADLINE: Duration = Duration::from_millis(400);
        let limits = Limits {
            answer_deadline: DEADLINE,
            ..Limits::default()
        };
        let mut rig = start("rj", limits, 0);
        rig.shared
            .lock()
            .set_connected(Some(HostId::try_from("sandbank").unwrap()));
        let (_, empty) = mpsc::channel(1);
        let mut commands = std::mem::replace(&mut rig.commands, empty);
        let rooms = [ticket(1), ticket(2), ticket(3)];
        // A session that answers the resolution after most of the deadline,
        // then holds every join without answering it.
        let answered = rooms.clone();
        let session = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Some(Command { op, reply }) = commands.recv().await {
                match op {
                    Op::Resolve { .. } => {
                        tokio::time::sleep(DEADLINE / 2).await;
                        let _ = reply.send(Response::Resolved {
                            rooms: answered
                                .iter()
                                .map(|room| local::ResolvedRoom {
                                    room: room.clone(),
                                    join: None,
                                })
                                .collect(),
                        });
                    }
                    _ => held.push(reply),
                }
            }
        });

        let started = Instant::now();
        let Response::Resolved { rooms: got } = ask(
            &rig,
            &Request::Resolve {
                repo: sealmesh_wire::RepoSlug::try_from("example-org/example-repo").unwrap(),
                ticket: None,
                join: true,
            },
        )
        .await
        else {
            panic!()
        };
        let took = started.elapsed();
        assert_eq!(got.len(), 3);
        assert!(
            got.iter().all(|r| r.join == Some(JoinOutcome::TimedOut)),
            "{got:?}"
        );
        // One deadline for the lot: not one per join after the resolution.
        assert!(took < DEADLINE + DEADLINE / 2, "took {took:?}");
        session.abort();
    }

    #[tokio::test]
    async fn an_endless_line_is_cut_off_at_the_cap_rather_than_read_forever() {
        let endless = tokio::io::repeat(b'a');
        let read = tokio::time::timeout(
            Duration::from_secs(5),
            read_line(endless, MAX_REQUEST_BYTES),
        )
        .await
        .expect("kept reading past the cap");
        assert!(read.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_read_stops_at_the_cap_plus_one() {
        let long = vec![b'a'; 100];
        let read = read_line(long.as_slice(), 10).await.unwrap();
        assert!(read.is_none());
        let fits = b"0123456789".to_vec();
        assert_eq!(read_line(fits.as_slice(), 10).await.unwrap().unwrap(), fits);
        let with_newline = b"012345678\n".to_vec();
        assert_eq!(
            read_line(with_newline.as_slice(), 10)
                .await
                .unwrap()
                .unwrap(),
            b"012345678"
        );
    }

    /// A join the session was handed: room, holder, and whether a lease.
    type SeenJoin = (RoomId, Option<ConsumerKey>, bool);

    /// A session that resolves every checkout to `rooms`, admits every join,
    /// and reports each join it was handed.
    fn joining_session(
        rig: &mut Rig,
        rooms: Vec<RoomId>,
    ) -> (
        tokio::task::JoinHandle<()>,
        mpsc::UnboundedReceiver<SeenJoin>,
    ) {
        rig.shared
            .lock()
            .set_connected(Some(HostId::try_from("sandbank").unwrap()));
        let (_, empty) = mpsc::channel(1);
        let mut commands = std::mem::replace(&mut rig.commands, empty);
        let (seen, joins) = mpsc::unbounded_channel();
        let session = tokio::spawn(async move {
            while let Some(Command { op, reply }) = commands.recv().await {
                let (room, holder, lease) = match op {
                    Op::Resolve { .. } => {
                        let _ = reply.send(Response::Resolved {
                            rooms: rooms
                                .iter()
                                .map(|room| local::ResolvedRoom {
                                    room: room.clone(),
                                    join: None,
                                })
                                .collect(),
                        });
                        continue;
                    }
                    Op::Hold(room, holder) => (room, holder, true),
                    Op::Join(room, holder) => (room, holder, false),
                    _ => continue,
                };
                let _ = seen.send((room.clone(), holder, lease));
                let _ = reply.send(Response::Joined { room });
            }
        });
        (session, joins)
    }

    async fn ask_raw(rig: &Rig, line: &[u8]) -> Response {
        let mut stream = UnixStream::connect(&rig.path).await.unwrap();
        stream.write_all(line).await.unwrap();
        let mut answer = Vec::new();
        stream.read_to_end(&mut answer).await.unwrap();
        local::decode(&answer, MAX_RESPONSE_BYTES).unwrap()
    }

    #[tokio::test]
    async fn an_older_clients_resolve_and_a_hold_both_join_under_a_lease() {
        let mut rig = start("hl", Limits::default(), 0);
        let (session, mut joins) = joining_session(&mut rig, vec![ticket(7)]);

        // Exactly the line a client built before `hold` sends.
        let old = br#"{"op":"resolve","repo":"example-org/example-repo","join":true}
"#;
        let Response::Resolved { rooms } = ask_raw(&rig, old).await else {
            panic!()
        };
        assert_eq!(rooms[0].join, Some(JoinOutcome::Joined));
        assert_eq!(joins.recv().await.unwrap(), (ticket(7), None, true));

        let holder = ConsumerKey::new("5f0c1f7e-session").unwrap();
        let Response::Resolved { rooms } = ask(
            &rig,
            &Request::Hold {
                repo: sealmesh_wire::RepoSlug::try_from("example-org/example-repo").unwrap(),
                ticket: Some(TicketId::new("ENG", 7).unwrap()),
                holder: holder.clone(),
            },
        )
        .await
        else {
            panic!()
        };
        assert_eq!(rooms[0].join, Some(JoinOutcome::Joined));
        assert_eq!(joins.recv().await.unwrap(), (ticket(7), Some(holder), true));

        // A resolution without `join` still joins nothing, and a plain
        // `join` is still a permanent claim.
        let plain = br#"{"op":"resolve","repo":"example-org/example-repo"}
"#;
        let Response::Resolved { rooms } = ask_raw(&rig, plain).await else {
            panic!()
        };
        assert_eq!(rooms[0].join, None);
        ask(
            &rig,
            &Request::Join {
                room: ticket(7),
                holder: None,
            },
        )
        .await;
        assert_eq!(joins.recv().await.unwrap(), (ticket(7), None, false));
        session.abort();
    }

    #[tokio::test]
    async fn a_hold_is_disabled_like_any_other_request_and_a_bad_holder_is_invalid() {
        let rig = start("hd", Limits::default(), 0);
        let bad = br#"{"op":"hold","repo":"example-org/example-repo","holder":"no spaces"}
"#;
        assert_eq!(ask_raw(&rig, bad).await, Response::Invalid {});
        let missing = br#"{"op":"hold","repo":"example-org/example-repo"}
"#;
        assert_eq!(ask_raw(&rig, missing).await, Response::Invalid {});
        rig.on.store(false, Ordering::SeqCst);
        let hold = br#"{"op":"hold","repo":"example-org/example-repo","holder":"s"}
"#;
        assert_eq!(ask_raw(&rig, hold).await, Response::Disabled {});
    }

    /// `Request` as a daemon built before `hold` decodes it, reduced to the
    /// one request that changed meaning.
    #[derive(Debug, serde::Deserialize)]
    #[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
    enum RequestBeforeHold {
        Resolve {
            #[allow(dead_code)]
            repo: sealmesh_wire::RepoSlug,
            #[serde(default)]
            #[allow(dead_code)]
            ticket: Option<TicketId>,
            #[serde(default)]
            #[allow(dead_code)]
            join: bool,
        },
    }

    #[test]
    fn a_daemon_built_before_hold_reads_resolve_the_same_and_refuses_hold_as_invalid() {
        let repo = sealmesh_wire::RepoSlug::try_from("example-org/example-repo").unwrap();
        let resolve = local::encode(
            &Request::Resolve {
                repo: repo.clone(),
                ticket: Some(TicketId::new("ENG", 7).unwrap()),
                join: true,
            },
            MAX_REQUEST_BYTES,
        )
        .unwrap();
        assert!(local::decode::<RequestBeforeHold>(&resolve, MAX_REQUEST_BYTES).is_ok());
        let hold = local::encode(
            &Request::Hold {
                repo,
                ticket: None,
                holder: ConsumerKey::new("s").unwrap(),
            },
            MAX_REQUEST_BYTES,
        )
        .unwrap();
        // Refused whole, which that daemon answers `invalid`: the caller can
        // tell and fall back to `resolve`, rather than being joined under a
        // claim that never expires.
        assert!(local::decode::<RequestBeforeHold>(&hold, MAX_REQUEST_BYTES).is_err());
    }
}
