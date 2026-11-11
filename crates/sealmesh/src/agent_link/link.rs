//! The HTTP side of `agents.sock`: authentication, routes, and the listener
//! that refuses every peer but this user. See the module above for the API.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result};
use axum::{
    Json, Router,
    body::{Body as HttpBody, Bytes},
    extract::{DefaultBodyLimit, Path as UrlPath, RawQuery, State, rejection::BytesRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use jiff::Timestamp;
use sealmesh_chat_wire::{Body, ChatWireError, MessageKind, RoomId, RoomName};
use sealmesh_proto::{BackfillRefusal, PostRefusal, RoomSeq, ServerFrame};
use sealmesh_server::rooms::Posted;
use sealmesh_wire::HostId;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{UnixListener, UnixStream},
    sync::{OwnedSemaphorePermit, Semaphore, broadcast::error::RecvError},
};

use super::{
    seat::{AgentMessage, Seat, SeatBusy},
    tokens::Tokens,
};
use crate::{
    audit::{self, AgentRefusal, RoomRecord},
    room::{Gate, server::peer_allowed},
};

/// The largest request body read, in bytes. A message body is at most 4,000
/// characters (16 KiB of UTF-8, more once JSON-escaped), so this leaves room
/// for the escaping and refuses anything far past it before it is buffered.
pub const MAX_REQUEST_BYTES: usize = 64 * 1024;

/// Connections served at once. One more is closed on accept. Each open stream
/// holds one, so this also bounds how many streams can be open.
pub const MAX_CONNECTIONS: usize = 32;

/// Refused posts one agent may have written to the audit ledger in a burst,
/// and how often it earns one back. Every accepted post is recorded; this
/// keeps a bot retrying in a tight loop from growing the ledger without bound.
const REFUSAL_NOTES_BURST: u32 = 30;
const REFUSAL_NOTE_REFILL: Duration = Duration::from_secs(2);

/// What the link serves from: who holds which token, each agent's seat, the
/// configured rooms, and the chat switch.
pub struct Link {
    tokens: Tokens,
    seats: BTreeMap<HostId, Seat>,
    members: BTreeMap<RoomName, Vec<HostId>>,
    gate: Gate,
    audit: Option<PathBuf>,
    refusal_notes: Mutex<BTreeMap<HostId, NoteBudget>>,
}

impl Link {
    pub fn new(
        tokens: Tokens,
        seats: Vec<Seat>,
        members: BTreeMap<RoomName, Vec<HostId>>,
        gate: Gate,
        audit: Option<PathBuf>,
    ) -> Self {
        Self {
            tokens,
            seats: seats
                .into_iter()
                .map(|seat| (seat.name.clone(), seat))
                .collect(),
            members,
            gate,
            audit,
            refusal_notes: Mutex::new(BTreeMap::new()),
        }
    }

    /// The seat whose token `headers` carries. A missing, malformed or
    /// unknown token is one answer.
    fn authenticate(&self, headers: &HeaderMap) -> Result<&Seat, ApiError> {
        let token = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::trim)
            .ok_or(ApiError::Unauthorized)?;
        let name = self.tokens.agent_for(token).ok_or(ApiError::Unauthorized)?;
        self.seats.get(name).ok_or(ApiError::Unauthorized)
    }

    fn enabled(&self) -> Result<(), ApiError> {
        if (self.gate)() {
            Ok(())
        } else {
            Err(ApiError::Disabled)
        }
    }

    /// Records one post attempt. An accepted post is always written; a
    /// refused one only while the agent has refusal notes left.
    fn note(&self, agent: &HostId, room: &RoomName, outcome: Result<RoomSeq, AgentRefusal>) {
        let Some(path) = &self.audit else { return };
        if outcome.is_err() && !self.spend_refusal_note(agent, Instant::now()) {
            return;
        }
        let (seq, refused) = match outcome {
            Ok(seq) => (Some(seq), None),
            Err(refusal) => (None, Some(refusal)),
        };
        let record = RoomRecord::AgentPost {
            at: Timestamp::now(),
            agent: agent.clone(),
            room: RoomId::named(room.clone()),
            seq,
            refused,
        };
        if let Err(error) = audit::record_room(path, &record) {
            eprintln!("could not write the audit log: {error:#}");
        }
    }

    fn spend_refusal_note(&self, agent: &HostId, now: Instant) -> bool {
        let mut notes = self
            .refusal_notes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        notes
            .entry(agent.clone())
            .or_insert_with(|| NoteBudget::new(now))
            .spend(now)
    }
}

struct NoteBudget {
    left: u32,
    refilled: Instant,
}

impl NoteBudget {
    fn new(now: Instant) -> Self {
        Self {
            left: REFUSAL_NOTES_BURST,
            refilled: now,
        }
    }

    fn spend(&mut self, now: Instant) -> bool {
        let earned = now.saturating_duration_since(self.refilled).as_nanos()
            / REFUSAL_NOTE_REFILL.as_nanos().max(1);
        if earned > 0 {
            let earned = u32::try_from(earned).unwrap_or(u32::MAX);
            self.left = self.left.saturating_add(earned).min(REFUSAL_NOTES_BURST);
            self.refilled = now;
        }
        if self.left == 0 {
            return false;
        }
        self.left -= 1;
        true
    }
}

/// Every way a request fails, as the word its body carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiError {
    /// No token, or one no configured agent holds.
    Unauthorized,
    /// A room this agent may not use, or that does not exist.
    NotMember,
    Invalid,
    TooLarge,
    RateLimited,
    BudgetExhausted,
    TurnCapped,
    NotRecorded,
    Archived,
    /// The agent's seat has too much in flight.
    Busy,
    /// Chat is off on this machine (`[chat] enabled`, or the kill file).
    Disabled,
    /// History could not be read, or the seat has stopped.
    Unavailable,
}

impl ApiError {
    pub fn status(self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::NotMember => StatusCode::FORBIDDEN,
            Self::Invalid => StatusCode::BAD_REQUEST,
            Self::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::RateLimited | Self::BudgetExhausted | Self::TurnCapped | Self::Busy => {
                StatusCode::TOO_MANY_REQUESTS
            }
            Self::Archived => StatusCode::CONFLICT,
            Self::NotRecorded | Self::Disabled | Self::Unavailable => {
                StatusCode::SERVICE_UNAVAILABLE
            }
        }
    }

    fn refusal(self) -> AgentRefusal {
        match self {
            Self::Unauthorized | Self::NotMember => AgentRefusal::NotMember,
            Self::Invalid => AgentRefusal::Invalid,
            Self::TooLarge => AgentRefusal::TooLarge,
            Self::RateLimited => AgentRefusal::RateLimited,
            Self::BudgetExhausted => AgentRefusal::BudgetExhausted,
            Self::TurnCapped => AgentRefusal::TurnCapped,
            Self::NotRecorded | Self::Unavailable => AgentRefusal::NotRecorded,
            Self::Archived => AgentRefusal::Archived,
            Self::Busy => AgentRefusal::Busy,
            Self::Disabled => AgentRefusal::Disabled,
        }
    }

    fn of_post(refusal: PostRefusal) -> Self {
        match refusal {
            PostRefusal::RateLimited => Self::RateLimited,
            PostRefusal::BudgetExhausted => Self::BudgetExhausted,
            PostRefusal::TurnCapped => Self::TurnCapped,
            PostRefusal::NotRecorded => Self::NotRecorded,
            PostRefusal::Archived => Self::Archived,
        }
    }

    fn of_seat(busy: SeatBusy) -> Self {
        match busy {
            SeatBusy::Full => Self::Busy,
            SeatBusy::Gone => Self::Unavailable,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (self.status(), Json(json!({ "error": self }))).into_response();
        if self == Self::Unauthorized {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}

/// A post's body. Nothing else: an author, a sequence number or a time is an
/// unknown field and refused, so a request cannot say who wrote it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PostRequest {
    body: String,
    #[serde(default)]
    kind: Option<MessageKind>,
}

#[derive(Debug, Serialize)]
struct RoomInfo<'a> {
    name: &'a RoomName,
    members: &'a [HostId],
}

pub fn router(link: Arc<Link>) -> Router {
    Router::new()
        .route("/v1/whoami", get(whoami))
        .route("/v1/rooms", get(rooms))
        .route("/v1/rooms/{name}/messages", get(backfill).post(post))
        .route("/v1/rooms/{name}/stream", get(stream))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(link)
}

/// The room `name` names, when `seat` may use it. A name that is not a room
/// name and a room this agent may not use are the same refusal.
fn usable(seat: &Seat, name: &str) -> Result<RoomName, ApiError> {
    RoomName::try_from(name)
        .ok()
        .filter(|room| seat.may_use(room))
        .ok_or(ApiError::NotMember)
}

async fn whoami(State(link): State<Arc<Link>>, headers: HeaderMap) -> Result<Response, ApiError> {
    let seat = link.authenticate(&headers)?;
    Ok(Json(json!({ "agent": seat.name, "rooms": seat.rooms })).into_response())
}

async fn rooms(State(link): State<Arc<Link>>, headers: HeaderMap) -> Result<Response, ApiError> {
    let seat = link.authenticate(&headers)?;
    let rooms: Vec<RoomInfo<'_>> = seat
        .rooms
        .iter()
        .map(|name| RoomInfo {
            name,
            members: link.members.get(name).map_or(&[], Vec::as_slice),
        })
        .collect();
    Ok(Json(json!({ "rooms": rooms })).into_response())
}

async fn post(
    State(link): State<Arc<Link>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Response, ApiError> {
    let seat = link.authenticate(&headers)?;
    let Ok(room) = RoomName::try_from(name.as_str()) else {
        return Err(ApiError::NotMember);
    };
    let outcome = post_as(&link, seat, &room, body).await;
    link.note(
        &seat.name,
        &room,
        outcome
            .as_ref()
            .map(|&seq| seq)
            .map_err(|error| error.refusal()),
    );
    let seq = outcome?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "room": room, "seq": seq, "author": seat.name })),
    )
        .into_response())
}

async fn post_as(
    link: &Link,
    seat: &Seat,
    room: &RoomName,
    body: Result<Bytes, BytesRejection>,
) -> Result<RoomSeq, ApiError> {
    if !seat.may_use(room) {
        return Err(ApiError::NotMember);
    }
    link.enabled()?;
    let bytes = body.map_err(|rejection| {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::TooLarge
        } else {
            ApiError::Invalid
        }
    })?;
    let request: PostRequest = serde_json::from_slice(&bytes).map_err(|_| ApiError::Invalid)?;
    let body = Body::new(request.body).map_err(|error| match error {
        ChatWireError::Body { chars: 0 } => ApiError::Invalid,
        ChatWireError::Body { .. } => ApiError::TooLarge,
        ChatWireError::RoomName => ApiError::Invalid,
    })?;
    let kind = request.kind.unwrap_or(MessageKind::Claim);
    match seat
        .post(room.clone(), kind, body)
        .await
        .map_err(ApiError::of_seat)?
    {
        Posted::Delivered { seq, .. } => Ok(seq),
        Posted::NotMember => Err(ApiError::NotMember),
        Posted::SequenceExhausted => Err(ApiError::NotRecorded),
        Posted::Refused { category, .. } => Err(ApiError::of_post(category)),
    }
}

/// `after=<seq>` and nothing else. An absent query reads from the start.
fn after(query: Option<&str>) -> Result<Option<RoomSeq>, ApiError> {
    let Some(query) = query.filter(|q| !q.is_empty()) else {
        return Ok(None);
    };
    let mut after = None;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').ok_or(ApiError::Invalid)?;
        if key != "after" || after.is_some() {
            return Err(ApiError::Invalid);
        }
        after = Some(RoomSeq(value.parse().map_err(|_| ApiError::Invalid)?));
    }
    Ok(after)
}

async fn backfill(
    State(link): State<Arc<Link>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
    RawQuery(query): RawQuery,
) -> Result<Response, ApiError> {
    let seat = link.authenticate(&headers)?;
    let room = usable(seat, &name)?;
    link.enabled()?;
    let after = after(query.as_deref())?;
    match seat
        .backfill(room.clone(), after)
        .await
        .map_err(ApiError::of_seat)?
    {
        ServerFrame::Backfill { messages, next, .. } => {
            let messages: Vec<AgentMessage> = messages
                .into_iter()
                .map(|entry| AgentMessage {
                    room: room.clone(),
                    seq: entry.seq,
                    author: entry.from,
                    kind: entry.kind,
                    body: entry.body,
                })
                .collect();
            Ok(Json(json!({ "room": room, "messages": messages, "next": next })).into_response())
        }
        ServerFrame::BackfillRefused { category, .. } => Err(match category {
            BackfillRefusal::NotHeld => ApiError::NotMember,
            BackfillRefusal::Busy => ApiError::Busy,
            BackfillRefusal::Unavailable => ApiError::Unavailable,
        }),
        _ => Err(ApiError::Unavailable),
    }
}

async fn stream(
    State(link): State<Arc<Link>>,
    headers: HeaderMap,
    UrlPath(name): UrlPath<String>,
) -> Result<Response, ApiError> {
    let seat = link.authenticate(&headers)?;
    let room = usable(seat, &name)?;
    link.enabled()?;
    // Subscribed before the response goes out, so nothing posted after the
    // headers arrive is missed.
    let receiver = seat.subscribe();
    let gate = link.gate.clone();
    let lines = futures_util::stream::unfold(
        (receiver, room, gate),
        |(mut receiver, room, gate)| async move {
            loop {
                match receiver.recv().await {
                    Ok(message) => {
                        // The kill file ends every stream at its next message.
                        if !gate() {
                            return None;
                        }
                        if message.room != room {
                            continue;
                        }
                        let mut line = serde_json::to_vec(&*message).ok()?;
                        line.push(b'\n');
                        return Some((
                            Ok::<_, std::convert::Infallible>(Bytes::from(line)),
                            (receiver, room, gate),
                        ));
                    }
                    // Fell more than a buffer behind, or the seat stopped:
                    // the stream ends, and the agent resumes with a backfill
                    // after the last seq it saw.
                    Err(RecvError::Lagged(_) | RecvError::Closed) => return None,
                }
            }
        },
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .header(header::CACHE_CONTROL, "no-store")
        .body(HttpBody::from_stream(lines))
        .map_err(|_| ApiError::Unavailable)
}

/// Counters a test can watch.
#[derive(Debug, Default)]
pub struct Stats {
    pub accepted: AtomicU64,
    pub refused_peer: AtomicU64,
    pub refused_full: AtomicU64,
}

/// Serves the link on `listener` until the process ends. `uid` is the only
/// uid a peer may have: the one that owns the socket.
pub async fn serve(
    listener: UnixListener,
    uid: u32,
    link: Arc<Link>,
    stats: Arc<Stats>,
) -> Result<()> {
    let listener = PeerChecked {
        inner: listener,
        uid,
        slots: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        stats,
    };
    axum::serve(listener, router(link))
        .await
        .context("serving agents.sock")
}

/// The Unix listener, accepting only this user, at most
/// [`MAX_CONNECTIONS`] at once. The same peer check as the daemon's own
/// socket.
struct PeerChecked {
    inner: UnixListener,
    uid: u32,
    slots: Arc<Semaphore>,
    stats: Arc<Stats>,
}

impl axum::serve::Listener for PeerChecked {
    type Io = Held;
    type Addr = tokio::net::unix::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, addr) = match self.inner.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    // Out of descriptors, most likely. Back off rather than spin.
                    eprintln!("agents.sock accept failed: {error}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            if !peer_allowed(&stream, self.uid) {
                self.stats.refused_peer.fetch_add(1, Ordering::SeqCst);
                eprintln!("agents.sock refused a connection not confirmed to be from this user");
                continue;
            }
            let Ok(slot) = self.slots.clone().try_acquire_owned() else {
                self.stats.refused_full.fetch_add(1, Ordering::SeqCst);
                continue;
            };
            self.stats.accepted.fetch_add(1, Ordering::SeqCst);
            return (
                Held {
                    stream,
                    _slot: slot,
                },
                addr,
            );
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// An accepted connection and the slot it holds until it closes.
struct Held {
    stream: UnixStream,
    _slot: OwnedSemaphorePermit,
}

impl AsyncRead for Held {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Held {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

/// A bare HTTP/1.1 client over a Unix socket, the way `curl --unix-socket`
/// talks to the link, for this crate's tests.
#[cfg(test)]
pub(crate) mod testing {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::{path::Path, time::Duration};

    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::UnixStream,
    };

    fn head(
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<&[u8]>,
        close: bool,
    ) -> String {
        let mut head = format!("{method} {path} HTTP/1.1\r\nHost: sealmesh\r\n");
        if close {
            head.push_str("Connection: close\r\n");
        }
        if let Some(token) = token {
            head.push_str(&format!("Authorization: Bearer {token}\r\n"));
        }
        if let Some(body) = body {
            head.push_str(&format!(
                "Content-Type: application/json\r\nContent-Length: {}\r\n",
                body.len()
            ));
        }
        head.push_str("\r\n");
        head
    }

    fn status(text: &str) -> u16 {
        text.split(' ')
            .nth(1)
            .and_then(|code| code.parse().ok())
            .unwrap_or(0)
    }

    /// One request, one answer: the status and the JSON body (`Null` when
    /// there is none).
    pub(crate) async fn request(
        socket: &Path,
        method: &str,
        path: &str,
        token: Option<&str>,
        body: Option<&[u8]>,
    ) -> (u16, serde_json::Value) {
        let mut stream = UnixStream::connect(socket).await.unwrap();
        stream
            .write_all(head(method, path, token, body, true).as_bytes())
            .await
            .unwrap();
        if let Some(body) = body {
            stream.write_all(body).await.unwrap();
        }
        let mut answer = Vec::new();
        tokio::time::timeout(Duration::from_secs(20), stream.read_to_end(&mut answer))
            .await
            .expect("no answer in time")
            .unwrap();
        let text = String::from_utf8_lossy(&answer).into_owned();
        let (headers, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
        let json = serde_json::from_str(body).unwrap_or(serde_json::Value::Null);
        (status(headers), json)
    }

    /// Opens a stream and reads up to the end of its headers.
    pub(crate) async fn open_stream(socket: &Path, path: &str, token: &str) -> (u16, UnixStream) {
        let mut stream = UnixStream::connect(socket).await.unwrap();
        stream
            .write_all(head("GET", path, Some(token), None, false).as_bytes())
            .await
            .unwrap();
        let mut seen = Vec::new();
        let mut byte = [0_u8; 1];
        while !seen.ends_with(b"\r\n\r\n") {
            let read = tokio::time::timeout(Duration::from_secs(20), stream.read(&mut byte))
                .await
                .expect("no headers in time")
                .unwrap();
            if read == 0 {
                break;
            }
            seen.extend_from_slice(&byte);
        }
        (status(&String::from_utf8_lossy(&seen)), stream)
    }

    /// Reads `stream` until it has said `needle`, and returns all it said.
    pub(crate) async fn read_until(stream: &mut UnixStream, needle: &str) -> String {
        let mut seen = String::new();
        let mut buffer = [0_u8; 4096];
        while !seen.contains(needle) {
            let read = tokio::time::timeout(Duration::from_secs(20), stream.read(&mut buffer))
                .await
                .unwrap_or_else(|_| panic!("never saw {needle:?} in {seen:?}"))
                .unwrap();
            assert!(read > 0, "the stream ended before {needle:?}: {seen:?}");
            seen.push_str(&String::from_utf8_lossy(buffer.get(..read).unwrap()));
        }
        seen
    }
}
