use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::Response,
    routing::get,
};
use sealmesh_proto::{
    BackfillRefusal, ClientFrame, MAX_FRAME_BYTES, PostRefusal, QueryId, Role, SecretToken,
    ServerFrame, decode,
};

use crate::{
    auth::{Denied, Directory, authenticate},
    check_budget::CheckBudgets,
    history::RoomHistory,
    projects::ProjectMap,
    query::{FANOUT_TIMEOUT, Narrowed, ask_readable, readable_projects},
    registry::Registry,
    rooms::{Posted, REVALIDATE_EVERY, Rooms},
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

/// Longest a write to a daemon's socket may take before the connection is
/// dropped. Room fan-out puts frames on a daemon's socket that the daemon did
/// not ask for, so a peer that has stopped reading must not hold its task —
/// and its memberships — forever. The same ten seconds the daemon allows its
/// own writes, and inside the load balancer's 60-second idle timeout.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// Posts one connection may have waiting behind the one being recorded.
///
/// A post is recorded before it is delivered, so it takes as long as a write,
/// and only one runs at a time per connection — that is what keeps delivery
/// order and stored order the same. The ones behind it have to go somewhere.
///
/// Refusing them instead was tried and is wrong: a host may post
/// [`POST_BURST`](crate::room_budget::POST_BURST) times at once, which is what
/// the token-budget design promises and what two socket tests already pin, and
/// a daemon is never told to wait for a post's answer the way
/// `ClientFrame::Join` tells it to wait for a join's. So a burst sent without
/// reading the answers is legal traffic, and refusing it would silently
/// withdraw the burst allowance.
///
/// The queue is therefore exactly one burst wide. A ninth concurrent post is
/// one the post rate would refuse anyway, so refusing it here says the same
/// thing one step earlier, and the bound on what a daemon can make the router
/// hold is a burst of bodies — each already capped by `Body`.
const MAX_PENDING_POSTS: usize = 8;

/// How long a daemon connection's last project map answers a repeat request
/// instead of a new cut. A cut makes up to `min(map repos, MAX_REPOS_PER_QUERY)`
/// `can_read` calls on the daemon's own token for whatever the access cache
/// does not hold, so a daemon re-sending `Projects` in a loop would otherwise
/// spend that on every request. Well under the daemon's own ten-minute
/// refresh (`room::client::MAP_REFRESH`), so a refresh always gets a fresh
/// cut; the reader path has no cooldown and does not need one, since a reader
/// asks once per `who`.
const PROJECTS_COOLDOWN: Duration = Duration::from_secs(60);

/// Bounds on the room re-check period, so a configured value can neither make
/// `tokio::time::interval` panic on zero (rule 39) nor overflow an `Instant`.
const REVALIDATE_FLOOR: Duration = Duration::from_millis(10);
const REVALIDATE_CEILING: Duration = Duration::from_secs(24 * 60 * 60);

pub struct App<D> {
    pub directory: D,
    pub registry: Registry,
    /// Loaded once from the router's own environment, never from a frame.
    pub projects: ProjectMap,
    /// Room membership. In-process like `registry`, and lost on deploy for the
    /// same reason.
    pub rooms: Rooms,
    /// Each reader's allowance of access checks GitHub has to answer, spent
    /// across every `Ask` that reader makes. In-process, lost on deploy.
    pub check_budgets: CheckBudgets,
    /// How often a daemon's rooms are re-checked against its own credential.
    /// `REVALIDATE_EVERY` unless a test shortens it. Each connection's first
    /// check is also offset by a jitter of up to half this period.
    pub room_revalidate_every: Duration,
    /// Longest a write to a daemon's socket may take before the connection is
    /// dropped. `WRITE_TIMEOUT` unless a test shortens it.
    pub daemon_write_timeout: Duration,
    /// Daemon connections dropped because a write to them stalled past
    /// `daemon_write_timeout`. Every one is also logged; the count is how a
    /// test sees the drop without reading stderr.
    pub stalled_writes: AtomicU64,
}

impl<D> App<D> {
    /// `history` is where room messages are recorded. It is required rather
    /// than defaulted: see [`Rooms`] on why there is no default store.
    pub fn new(directory: D, projects: ProjectMap, history: Arc<dyn RoomHistory>) -> Self {
        Self::with_rooms(directory, projects, Rooms::new(history))
    }

    /// [`App::new`] over a room registry the caller built, for a router whose
    /// room limits come from its environment rather than their defaults.
    pub fn with_rooms(directory: D, projects: ProjectMap, rooms: Rooms) -> Self {
        Self {
            directory,
            registry: Registry::new(),
            projects,
            rooms,
            check_budgets: CheckBudgets::default(),
            room_revalidate_every: REVALIDATE_EVERY,
            daemon_write_timeout: WRITE_TIMEOUT,
            stalled_writes: AtomicU64::new(0),
        }
    }
}

pub fn router<D: Directory + 'static>(app: Arc<App<D>>) -> Router {
    Router::new()
        .route("/v1/connect", get(connect::<D>))
        .route("/v1/healthz", get(healthz))
        .with_state(Session { app })
}

struct Session<D> {
    app: Arc<App<D>>,
}

impl<D> Clone for Session<D> {
    fn clone(&self) -> Self {
        Self {
            app: Arc::clone(&self.app),
        }
    }
}

async fn healthz() -> StatusCode {
    StatusCode::OK
}

async fn connect<D: Directory + 'static>(
    upgrade: WebSocketUpgrade,
    State(session): State<Session<D>>,
) -> Response {
    upgrade
        .max_message_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| run(socket, session))
}

async fn run<D: Directory>(mut socket: WebSocket, session: Session<D>) {
    let Some((host, role, token)) =
        within(HANDSHAKE_TIMEOUT, handshake(&mut socket, &session)).await
    else {
        let _ = send(&mut socket, &ServerFrame::Denied {}).await;
        return;
    };

    if send(&mut socket, &ServerFrame::Welcome { host: host.clone() })
        .await
        .is_err()
    {
        return;
    }

    // Both roles keep the token for as long as this socket is open: a reader's
    // requests resolve repo access with its own credential, and so do a
    // daemon's room joins and their periodic re-checks. In memory only, owned
    // by this connection's task — never put in a shared structure, never
    // persisted, never logged (rule 7).
    match role {
        Role::Daemon => serve_daemon(socket, &session.app, host, token).await,
        Role::Reader => serve_reader(socket, &session.app, host, token).await,
    }
}

async fn within<T>(limit: Duration, step: impl Future<Output = Option<T>>) -> Option<T> {
    tokio::time::timeout(limit, step).await.ok().flatten()
}

async fn handshake<D: Directory>(
    socket: &mut WebSocket,
    session: &Session<D>,
) -> Option<(sealmesh_wire::HostId, Role, SecretToken)> {
    let Received::Frame(frame) = recv::<ClientFrame>(socket).await else {
        return None;
    };
    let (role, token) = credential(frame)?;

    match authenticate(&session.app.directory, &token).await {
        Ok(host) => Some((host, role, token)),
        Err(Denied::NoCredential | Denied::NotAuthorised) => None,
    }
}

/// The first frame on a socket, read as a handshake. Anything but `Hello` —
/// a room frame included — is `None`, and the connection is denied without
/// that frame being acted on (rules 2, 20).
fn credential(frame: ClientFrame) -> Option<(Role, SecretToken)> {
    match frame {
        ClientFrame::Hello { role, token } => Some((role, token)),
        _ => None,
    }
}

/// The one access check a daemon connection may have in flight: a join's
/// answer, or a re-check's `Left` frames.
///
/// It borrows the connection task's own token, membership and directory, and
/// is polled inside that task's `select!` beside everything else. So a check
/// waiting on a slow GitHub never stops the task reading `Answer`s or draining
/// the query outbox and the room queue — a stuck check made the machine read
/// as unreachable to every `who` — and the token is never cloned or moved into
/// a spawned task.
type AccessCheck<'a> = std::pin::Pin<Box<dyn Future<Output = Vec<ServerFrame>> + Send + 'a>>;

/// Polls the running check, or waits forever when there is none. Cancel-safe:
/// the check lives in `check`, so a `select!` that picks another branch drops
/// only this borrow of it.
async fn in_flight(check: &mut Option<AccessCheck<'_>>) -> Vec<ServerFrame> {
    match check {
        Some(running) => running.as_mut().await,
        None => std::future::pending().await,
    }
}

/// One post, from its limit checks through the history write to the fan-out,
/// pinned into the connection's `select!`.
///
/// Same shape and the same reason as [`AccessCheck`]. A post waits on the
/// store, and awaiting it inline in an arm body would run that body to
/// completion before the loop could iterate — so for as long as the write took,
/// this task could not drain the query outbox, deliver a room message, or read
/// the `Answer` that resolves a query. A slow table would then make every
/// posting daemon read as unreachable to every `who`, which is a chat outage
/// becoming a `who` outage.
type PostInFlight<'a> = std::pin::Pin<Box<dyn Future<Output = Posted> + Send + 'a>>;

/// Polls the running post, or waits forever when there is none. Cancel-safe
/// the way [`in_flight`] is: the future lives in `posting`, so a `select!` that
/// picks another branch drops only this borrow of it — and nothing of the
/// post's place in the ingest queue, which is what keeps delivery order and
/// stored order the same.
async fn in_flight_post(posting: &mut Option<PostInFlight<'_>>) -> Posted {
    match posting {
        Some(running) => running.as_mut().await,
        None => std::future::pending().await,
    }
}

/// A backfill read or a room listing, pinned into the connection's `select!`.
///
/// Same shape and the same reason as [`AccessCheck`] and [`PostInFlight`]: a
/// backfill waits on the store and a listing waits on GitHub, and either
/// awaited in an arm body would park this connection — no `Query` written, no
/// `Answer` read — for as long as it took, so the machine would read as
/// unreachable to every `who`. One of each runs at a time per connection.
type ReadInFlight<'a> = std::pin::Pin<Box<dyn Future<Output = ServerFrame> + Send + 'a>>;

/// Polls the running read, or waits forever when there is none. Cancel-safe
/// the way [`in_flight`] is.
async fn in_flight_read(read: &mut Option<ReadInFlight<'_>>) -> ServerFrame {
    match read {
        Some(running) => running.as_mut().await,
        None => std::future::pending().await,
    }
}

/// One post waiting behind the one being recorded.
type PendingPost = (
    sealmesh_chat_wire::RoomId,
    sealmesh_chat_wire::MessageKind,
    sealmesh_chat_wire::Body,
);

/// Cancels this connection's posts to `key` that have not been recorded yet,
/// and reports how many were dropped.
///
/// A post must never be recorded against a membership later than the one it
/// was read under. **Two paths end a membership, and both have to cancel:** a
/// `ClientFrame::Leave`, and a re-check revocation, which removes membership
/// and emits `Left` with no client frame involved at all. Miss either and a
/// post still parked on the ingest lock can reserve against the room's next
/// incarnation once a rejoin admits, filing a message from one life of a room
/// into the next.
///
/// The queued ones go because a post that starts after a rejoin admits would
/// do exactly that. The one in flight goes because `select!` need not have
/// polled its branch yet, so it may not have run a single line; dropping the
/// future is what stops it, since it admits nothing until it runs. Plain
/// removal without a rejoin needs none of this — `reserve` already finds no
/// membership.
///
/// They are dropped with no answer, like any other post to a room this
/// connection is not in, and counted as stray so the disconnect log says so.
/// A daemon that wants its post recorded waits for the answer before leaving,
/// the discipline `ClientFrame::Join` already asks for.
///
/// **The cost runs in both directions.** A post sent and then abandoned by an
/// immediate leave may never be recorded. And cancelling one whose write had
/// already resolved leaves that row in the table delivered to nobody — the
/// same shape as a write whose response was lost, which the
/// burn-don't-roll-back rule already accounts for. History may therefore hold
/// a message no live member received; a late joiner reads it from there.
fn cancel_posts_to(
    key: &sealmesh_chat_wire::RoomId,
    projects: &ProjectMap,
    pending: &mut VecDeque<PendingPost>,
    posting: &mut Option<PostInFlight<'_>>,
    posting_room: &mut Option<sealmesh_chat_wire::RoomId>,
) -> u64 {
    let held = pending.len();
    pending.retain(|(queued, _, _)| {
        crate::rooms::Membership::room_key(projects, queued).as_ref() != Some(key)
    });
    let mut cancelled = u64::try_from(held.saturating_sub(pending.len())).unwrap_or(0);
    if posting_room.as_ref() == Some(key) {
        *posting = None;
        *posting_room = None;
        cancelled = cancelled.saturating_add(1);
    }
    cancelled
}

async fn serve_daemon<D: Directory>(
    mut socket: WebSocket,
    app: &Arc<App<D>>,
    host: sealmesh_wire::HostId,
    token: SecretToken,
) {
    let (mut outbox, _attached) = app.registry.attach(host.clone());
    let (mut deliveries, membership) = app.rooms.connect(host.clone());
    let mut revalidate = revalidation_timer(app.room_revalidate_every, jitter_seed(&host));

    // One access check at a time per connection, which bounds the GitHub calls
    // one daemon can have running to one join's worth. A `Join` that arrives
    // while a check is running is refused straight away — the same coarse
    // `JoinRefused`, and the daemon may ask again once it has its answer. A
    // re-check that falls due waits for the running check and starts next.
    let mut check: Option<AccessCheck<'_>> = None;
    let mut recheck_due = false;
    // One post recorded at a time per connection, so delivery order is the
    // order history has, with the ones behind it in a bounded queue. Reading a
    // `Post` only ever costs a push, so waiting on the store never stops this
    // task answering a query or delivering a message.
    let mut posting: Option<PostInFlight<'_>> = None;
    let mut pending: VecDeque<PendingPost> = VecDeque::new();
    // The room the post being recorded belongs to, as the registry keys it,
    // so that anything ending that membership can cancel the post. Both
    // paths that end one do: a `Leave` frame, and a re-check revocation. See
    // `cancel_posts_to`.
    let mut posting_room: Option<sealmesh_chat_wire::RoomId> = None;
    // The room of the join `check` is running, when it is a join: as the
    // daemon sent it, and as the registry keys it. A `Leave` for that room
    // cancels the join. `None` while no check runs, or while it is a re-check.
    let mut joining: Option<(
        sealmesh_chat_wire::RoomId,
        Option<sealmesh_chat_wire::RoomId>,
    )> = None;

    // One history read and one room listing at a time per connection, each
    // in its own branch. A second of either while one runs is refused at once
    // rather than queued, the way a second join is.
    let mut backfilling: Option<ReadInFlight<'_>> = None;
    let mut listing: Option<ReadInFlight<'_>> = None;
    // The project map, cut to what this daemon's own token can read, exactly
    // as a reader's is. Its own branch because the cut waits on GitHub. One
    // at a time: a second request while one runs is answered by the reply
    // already coming, and one within `PROJECTS_COOLDOWN` of the last answer
    // gets that answer again rather than a new cut.
    let mut mapping: Option<ReadInFlight<'_>> = None;
    let mut last_map: Option<(tokio::time::Instant, ServerFrame)> = None;
    let mut repeated_projects: u64 = 0;

    let mut events_dropped: u64 = 0;
    let mut refused_backfills: u64 = 0;
    let mut busy_backfills: u64 = 0;
    let mut busy_listings: u64 = 0;
    let mut unhandled_frames: u64 = 0;
    let mut undecodable_frames: u64 = 0;
    let mut stray_room_frames: u64 = 0;
    let mut exhausted_posts: u64 = 0;
    let mut rate_refused_posts: u64 = 0;
    let mut budget_refused_posts: u64 = 0;
    let mut turn_capped_posts: u64 = 0;
    let mut not_recorded_posts: u64 = 0;
    let mut archived_posts: u64 = 0;
    let mut busy_refused_posts: u64 = 0;

    loop {
        if check.is_none() && recheck_due {
            recheck_due = false;
            check = Some(Box::pin(membership.revalidate(
                &app.directory,
                &app.projects,
                &token,
            )));
        }

        // The next queued post starts as soon as the last one finishes, the
        // same shape as a re-check that fell due while a join was running.
        // A let-chain, so `pop_front` runs only when nothing is in flight.
        if posting.is_none()
            && let Some((room, kind, body)) = pending.pop_front()
        {
            // `async move` so the future owns the room: the frame's binding is
            // long gone, and `member` is a shared reference, so the move
            // copies that rather than taking the membership the other branches
            // still need.
            // Keyed before the room moves into the future, so a `Leave` can
            // tell whether it cancels this post.
            posting_room = crate::rooms::Membership::room_key(&app.projects, &room);
            let member = &membership;
            // The post borrows this task's own token for a ticket room's merge
            // check. Borrowed, like the join check's: never cloned, never moved
            // into a spawned task or anything shared. And the check is inside
            // this pinned future, so waiting on GitHub parks this branch only.
            let token = &token;
            posting = Some(Box::pin(async move {
                member
                    .post(&app.directory, &app.projects, token, &room, kind, body)
                    .await
            }));
        }

        tokio::select! {
            queued = outbox.recv() => {
                let Some(frame) = queued else { break };
                if send_within(&mut socket, app, &host, &frame).await.is_err() {
                    break;
                }
            }
            // A message queued for a room this connection has since left, or
            // posted before its current join, is skipped here rather than sent.
            delivered = membership.next_delivery(&mut deliveries) => {
                let Some(frame) = delivered else { break };
                if send_within(&mut socket, app, &host, &frame).await.is_err() {
                    break;
                }
            }
            _ = revalidate.tick() => {
                recheck_due = true;
            }
            finished = in_flight(&mut check), if check.is_some() => {
                check = None;
                joining = None;
                // A re-check that revoked a room ends that membership without
                // any client frame, so it has to cancel this connection's
                // unrecorded posts there exactly as a `Leave` does. `Left` is
                // the only outcome that ends one, and it already carries the
                // registry's key.
                for frame in &finished {
                    if let ServerFrame::Left { room } = frame {
                        stray_room_frames = stray_room_frames.saturating_add(cancel_posts_to(
                            room,
                            &app.projects,
                            &mut pending,
                            &mut posting,
                            &mut posting_room,
                        ));
                    }
                }
                if send_all(&mut socket, app, &host, &finished).await.is_err() {
                    break;
                }
            }
            // The post's own branch, so waiting on the store never stops this
            // task doing anything else. Its outcome is answered here rather
            // than where the frame arrived.
            posted = in_flight_post(&mut posting), if posting.is_some() => {
                posting = None;
                posting_room = None;
                match posted {
                    Posted::Delivered { .. } => {}
                    Posted::NotMember => {
                        stray_room_frames = stray_room_frames.saturating_add(1);
                    }
                    Posted::SequenceExhausted => {
                        exhausted_posts = exhausted_posts.saturating_add(1);
                    }
                    // Answered every time, never dropped silently: an agent
                    // that cannot tell it was muted keeps trying.
                    Posted::Refused { room, category } => {
                        match category {
                            PostRefusal::RateLimited => {
                                rate_refused_posts = rate_refused_posts.saturating_add(1);
                            }
                            PostRefusal::BudgetExhausted => {
                                budget_refused_posts = budget_refused_posts.saturating_add(1);
                            }
                            PostRefusal::TurnCapped => {
                                turn_capped_posts = turn_capped_posts.saturating_add(1);
                            }
                            PostRefusal::NotRecorded => {
                                not_recorded_posts = not_recorded_posts.saturating_add(1);
                            }
                            PostRefusal::Archived => {
                                archived_posts = archived_posts.saturating_add(1);
                            }
                        }
                        let refused = ServerFrame::PostRefused { room, category };
                        if send_within(&mut socket, app, &host, &refused).await.is_err() {
                            break;
                        }
                    }
                }
            }
            // The store read's own branch. Its answer is sent here, not where
            // the request arrived.
            read = in_flight_read(&mut backfilling), if backfilling.is_some() => {
                backfilling = None;
                if let ServerFrame::BackfillRefused {
                    category: BackfillRefusal::NotHeld,
                    ..
                } = read
                {
                    refused_backfills = refused_backfills.saturating_add(1);
                }
                if send_within(&mut socket, app, &host, &read).await.is_err() {
                    break;
                }
            }
            // The listing's own branch, for the same reason: its checks wait
            // on GitHub.
            listed = in_flight_read(&mut listing), if listing.is_some() => {
                listing = None;
                if send_within(&mut socket, app, &host, &listed).await.is_err() {
                    break;
                }
            }
            // The project map's own branch, for the same reason.
            mapped = in_flight_read(&mut mapping), if mapping.is_some() => {
                mapping = None;
                let sent = send_within(&mut socket, app, &host, &mapped).await;
                last_map = Some((tokio::time::Instant::now(), mapped));
                if sent.is_err() {
                    break;
                }
            }
            incoming = recv::<ClientFrame>(&mut socket) => {
                match incoming {
                    Received::Frame(ClientFrame::Answer { id, state }) => {
                        app.registry.resolve(id, state);
                    }
                    Received::Frame(ClientFrame::Events { batch }) => {
                        events_dropped = events_dropped
                            .saturating_add(u64::try_from(batch.len()).unwrap_or(u64::MAX));
                    }
                    Received::Frame(ClientFrame::Join { room }) => {
                        // A room this connection already holds is answered
                        // `Joined` straight away: no access check, no rate
                        // limit, and `joining` stays unset, so a `Leave` that
                        // follows is an ordinary leave. Revocation of a held
                        // room is the periodic re-check's job.
                        let held = crate::rooms::Membership::room_key(&app.projects, &room)
                            .filter(|key| membership.holds(key));
                        if let Some(held) = held {
                            let joined = ServerFrame::Joined { room: held };
                            if send_within(&mut socket, app, &host, &joined).await.is_err() {
                                break;
                            }
                        } else if check.is_some() {
                            let refused = membership.refuse_busy(room);
                            if send_within(&mut socket, app, &host, &refused).await.is_err() {
                                break;
                            }
                        } else {
                            let key = crate::rooms::Membership::room_key(&app.projects, &room);
                            joining = Some((room.clone(), key));
                            let join = membership.join(&app.directory, &app.projects, &token, room);
                            check = Some(Box::pin(async move { vec![join.await] }));
                        }
                    }
                    Received::Frame(ClientFrame::Leave { room }) => {
                        // A post read before this `Leave` must not be recorded
                        // after it, and least of all after a rejoin. The same
                        // cancellation a re-check revocation runs, because
                        // both end a membership — see `cancel_posts_to`.
                        if let Some(key) = crate::rooms::Membership::room_key(&app.projects, &room)
                        {
                            stray_room_frames = stray_room_frames.saturating_add(cancel_posts_to(
                                &key,
                                &app.projects,
                                &mut pending,
                                &mut posting,
                                &mut posting_room,
                            ));
                        }
                        let names_the_join = joining.as_ref().is_some_and(|(asked, key)| {
                            *asked == room
                                || (key.is_some()
                                    && *key
                                        == crate::rooms::Membership::room_key(&app.projects, &room))
                        });
                        let cancelled = if names_the_join { joining.take() } else { None };
                        if let Some((asked, _)) = cancelled {
                            // A join admits only when its check completes, so
                            // dropping the unfinished check means it never
                            // admits — even if GitHub answers a moment later.
                            check = None;
                            let refused = membership.refuse_cancelled(asked);
                            if send_within(&mut socket, app, &host, &refused).await.is_err() {
                                break;
                            }
                        } else {
                            match membership.leave(&app.projects, &room) {
                                Some(left) => {
                                    if send_within(&mut socket, app, &host, &left).await.is_err() {
                                        break;
                                    }
                                }
                                // A room this connection is not in: dropped,
                                // like an `Answer` for a ticket the registry
                                // never issued (rule 20).
                                None => stray_room_frames = stray_room_frames.saturating_add(1),
                            }
                        }
                    }
                    Received::Frame(ClientFrame::Post { room, kind, body }) => {
                        // Only ever a push, never a wait: whatever the store
                        // is doing, reading a `Post` cannot park this task.
                        // The queue is a full burst wide, so a legal burst
                        // pipelined without reading the answers is admitted.
                        if pending.len() >= MAX_PENDING_POSTS {
                            // Past a burst, and refused at once rather than
                            // held. `RateLimited` rather than a new category:
                            // what it documents — "posting faster than the
                            // router allows, recovers within seconds" — is
                            // exactly true, the remedy is identical, and the
                            // post rate would refuse this post a moment later
                            // anyway. A new category would be a wire change
                            // every installed daemon must learn to read
                            // something it can already act on. The precise
                            // reason goes to the log (rule 28), the way each
                            // join refusal's does.
                            match crate::rooms::Membership::room_key(&app.projects, &room) {
                                Some(key) => {
                                    busy_refused_posts = busy_refused_posts.saturating_add(1);
                                    let refused = ServerFrame::PostRefused {
                                        room: key,
                                        category: PostRefusal::RateLimited,
                                    };
                                    if send_within(&mut socket, app, &host, &refused)
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                // No key means no such room, so this
                                // connection is not in it: dropped with no
                                // answer, like any other post to a room it is
                                // not a member of.
                                None => {
                                    stray_room_frames = stray_room_frames.saturating_add(1);
                                }
                            }
                        } else {
                            pending.push_back((room, kind, body));
                        }
                    }
                    Received::Frame(ClientFrame::Backfill { room, after }) => {
                        // Only ever starts the read, never waits on it.
                        if backfilling.is_some() {
                            busy_backfills = busy_backfills.saturating_add(1);
                            let refused = membership.refuse_backfill_busy(room);
                            if send_within(&mut socket, app, &host, &refused).await.is_err() {
                                break;
                            }
                        } else {
                            let member = &membership;
                            let projects = &app.projects;
                            backfilling = Some(Box::pin(async move {
                                member.backfill(projects, room, after).await
                            }));
                        }
                    }
                    Received::Frame(ClientFrame::Projects {}) => {
                        // Only ever starts the cut, never waits on it. The
                        // same answer a reader gets, from the same function,
                        // so a daemon learns nothing about a project whose
                        // repos its token cannot read (rule 25). Not charged
                        // to the reader check allowance, like the reader's,
                        // because it checks the router's own map rather than
                        // repos a peer named — but it is not free: each cut
                        // makes up to `min(map repos, MAX_REPOS_PER_QUERY)`
                        // `can_read` calls on this daemon's own token for
                        // whatever the access cache does not hold, which is
                        // what the cooldown bounds.
                        let recent = last_map.as_ref().filter(|(at, _)| {
                            at.elapsed() < PROJECTS_COOLDOWN
                        });
                        if mapping.is_some() {
                            repeated_projects = repeated_projects.saturating_add(1);
                        } else if let Some((_, answer)) = recent {
                            repeated_projects = repeated_projects.saturating_add(1);
                            if send_within(&mut socket, app, &host, answer).await.is_err() {
                                break;
                            }
                        } else {
                            let (asked_by, token) = (&host, &token);
                            mapping = Some(Box::pin(async move {
                                answer_projects(app, asked_by, token).await
                            }));
                        }
                    }
                    Received::Frame(ClientFrame::ListRooms {}) => {
                        if listing.is_some() {
                            busy_listings = busy_listings.saturating_add(1);
                            let refused = ServerFrame::RoomListRefused {};
                            if send_within(&mut socket, app, &host, &refused).await.is_err() {
                                break;
                            }
                        } else {
                            // Borrows this task's own token, like a join's
                            // check: never cloned, never moved into anything
                            // shared or spawned.
                            listing = Some(Box::pin(membership.list_rooms(
                                &app.directory,
                                &app.projects,
                                &token,
                                &app.check_budgets,
                            )));
                        }
                    }
                    Received::Frame(_) => {
                        unhandled_frames = unhandled_frames.saturating_add(1);
                    }
                    // A frame this build cannot read is not a closed socket.
                    // Dropping the connection here would make a daemon that is
                    // merely newer report as unreachable on every reader.
                    Received::Undecodable => {
                        undecodable_frames = undecodable_frames.saturating_add(1);
                    }
                    Received::Closed => break,
                }
            }
        }
    }

    if events_dropped > 0 {
        eprintln!(
            "sealmesh-server: dropped {events_dropped} events from {host} — the events \
             channel has no consumer yet"
        );
    }
    if undecodable_frames > 0 {
        eprintln!(
            "sealmesh-server: could not read {undecodable_frames} frames from {host} — that \
             daemon is probably running a build this router does not know. Deploy the router \
             before the daemons."
        );
    }
    if unhandled_frames > 0 {
        eprintln!("sealmesh-server: ignored {unhandled_frames} unexpected frames from {host}");
    }
    if stray_room_frames > 0 {
        eprintln!(
            "sealmesh-server: dropped {stray_room_frames} room frames from {host} naming a room \
             it was not in"
        );
    }
    if exhausted_posts > 0 {
        eprintln!(
            "sealmesh-server: dropped {exhausted_posts} posts from {host} to a room that has \
             used every sequence number"
        );
    }
    if rate_refused_posts > 0 || budget_refused_posts > 0 || turn_capped_posts > 0 {
        eprintln!(
            "sealmesh-server: refused {rate_refused_posts} posts from {host} over the post rate \
             limit, {budget_refused_posts} over a room token allowance and {turn_capped_posts} \
             over a room's turn cap"
        );
    }
    if not_recorded_posts > 0 {
        eprintln!(
            "sealmesh-server: refused {not_recorded_posts} posts from {host} because they could \
             not be written to the room's history, and delivered none of them"
        );
    }
    // The archive itself is logged once, when it happens; the refusals it
    // causes are counted here rather than logged one by one (rule 28).
    if archived_posts > 0 {
        eprintln!(
            "sealmesh-server: refused {archived_posts} posts from {host} to ticket rooms that \
             are archived because their pull request merged"
        );
    }
    // Answered as `rate_limited` on the wire, but a distinct reason here, the
    // way each join refusal is logged with its own (rule 28).
    if busy_refused_posts > 0 {
        eprintln!(
            "sealmesh-server: refused {busy_refused_posts} posts from {host} that arrived while \
             this connection's previous post was still being recorded"
        );
    }
    // Rule 28: refusals are counted and logged, never the room a refused
    // request named (rule 13).
    if refused_backfills > 0 {
        eprintln!(
            "sealmesh-server: refused {refused_backfills} backfills from {host} of rooms it did \
             not hold"
        );
    }
    if repeated_projects > 0 {
        eprintln!(
            "sealmesh-server: answered {repeated_projects} project map requests from {host} \
             without a new cut, because one was running or had just been answered"
        );
    }
    if busy_backfills > 0 || busy_listings > 0 {
        eprintln!(
            "sealmesh-server: refused {busy_backfills} backfills and {busy_listings} room \
             listings from {host} that arrived while its previous one was still running"
        );
    }
    let undelivered = membership.undelivered();
    if undelivered > 0 {
        eprintln!(
            "sealmesh-server: {host} missed {undelivered} room messages because it was reading \
             more slowly than its rooms were posting"
        );
    }
}

/// The timer a daemon connection re-checks its rooms on.
///
/// The first check lands one full period after connecting, since every room
/// was checked on join, plus a jitter of up to half a period drawn from
/// `seed`. After a deploy every daemon reconnects within seconds; without the
/// jitter they would all re-check at the same instant, every period, and the
/// burst of GitHub calls would repeat for as long as the router stays up.
/// Later checks keep the same period, so connections stay spread out.
fn revalidation_timer(every: Duration, seed: u64) -> tokio::time::Interval {
    let every = every.clamp(REVALIDATE_FLOOR, REVALIDATE_CEILING);
    let delay = every.saturating_add(jitter(every, seed));
    let now = tokio::time::Instant::now();
    let first = now.checked_add(delay).unwrap_or(now);
    let mut timer = tokio::time::interval_at(first, every);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    timer
}

/// A jitter in `[0, every / 2)` picked by `seed`. Zero for a period too short
/// to split, so it can never divide by zero (rule 40).
fn jitter(every: Duration, seed: u64) -> Duration {
    let span = u64::try_from((every / 2).as_nanos()).unwrap_or(u64::MAX);
    if span == 0 {
        return Duration::ZERO;
    }
    Duration::from_nanos(seed % span)
}

/// A per-connection seed for `jitter`: the connection's host and the instant
/// it registered, hashed with the standard library's randomly keyed hasher.
///
/// Spread, not secrecy — nothing about it needs to be unpredictable. The
/// workspace declares `rand` but no crate uses it, and a dependency added only
/// to scatter a timer is surface this does not need (rule 30).
fn jitter_seed(host: &sealmesh_wire::HostId) -> u64 {
    use std::hash::{BuildHasher as _, Hash as _, Hasher as _};

    let registered = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default();
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    host.hash(&mut hasher);
    registered.hash(&mut hasher);
    hasher.finish()
}

async fn serve_reader<D: Directory>(
    mut socket: WebSocket,
    app: &Arc<App<D>>,
    asked_by: sealmesh_wire::HostId,
    token: SecretToken,
) {
    loop {
        let frame = match recv::<ClientFrame>(&mut socket).await {
            Received::Frame(frame) => frame,
            Received::Undecodable => continue,
            Received::Closed => return,
        };

        match frame {
            ClientFrame::Projects {} => {
                let reply = answer_projects(app, &asked_by, &token).await;
                if send(&mut socket, &reply).await.is_err() {
                    return;
                }
            }
            ClientFrame::Ask { id } => {
                for frame in answer_ask(app, &asked_by, &token, id).await {
                    if send(&mut socket, &frame).await.is_err() {
                        return;
                    }
                }
            }
            _ => continue,
        }
    }
}

/// A reader's `Ask`, answered: the fan-out narrowed to what `token` can read
/// (rule 4), in the frame order `answer_frames` fixes.
///
/// This is where the authorization boundary is enforced, so it is what the
/// tests exercise — not only the narrowing helpers underneath it, which could
/// all be correct while this skipped them.
async fn answer_ask<D: Directory>(
    app: &App<D>,
    asked_by: &sealmesh_wire::HostId,
    token: &SecretToken,
    id: QueryId,
) -> Vec<ServerFrame> {
    let narrowed = ask_readable(
        &app.registry,
        &app.directory,
        &app.check_budgets,
        token,
        asked_by,
        FANOUT_TIMEOUT,
    )
    .await;
    answer_frames(id, narrowed)
}

/// A `Projects` request, answered — a reader's, or a daemon's resolving its
/// checkouts' rooms: the router's mapping filtered the same way as the
/// fan-out, through the same cached resolver, so a project whose repos this
/// caller cannot read is not in the reply at all (rule 25).
async fn answer_projects<D: Directory>(
    app: &App<D>,
    asked_by: &sealmesh_wire::HostId,
    token: &SecretToken,
) -> ServerFrame {
    let projects = app.projects.to_wire();
    let projects = readable_projects(&app.directory, token, asked_by, projects).await;
    ServerFrame::Projects { projects }
}

/// Every frame one `Ask` is answered with, in the order they go out: one
/// `Report` per machine, then `Partial` if anything was withheld, then
/// `Complete`.
///
/// `Partial` comes before `Complete`, never after. A reader built before
/// `Partial` existed stops listening at `Complete`, and it skips a frame it
/// cannot decode only while it is still reading — so this order is what makes
/// the new frame harmless to every installed `who`.
fn answer_frames(id: QueryId, narrowed: Narrowed) -> Vec<ServerFrame> {
    let Narrowed { reports, partial } = narrowed;
    let mut frames: Vec<ServerFrame> = reports
        .into_iter()
        .map(|(host, state)| ServerFrame::Report { id, host, state })
        .collect();
    if partial {
        frames.push(ServerFrame::Partial { id });
    }
    frames.push(ServerFrame::Complete { id });
    frames
}

async fn send(socket: &mut WebSocket, frame: &ServerFrame) -> Result<(), ()> {
    let body = serde_json::to_string(frame).map_err(|_| ())?;
    socket
        .send(Message::Text(body.into()))
        .await
        .map_err(|_| ())
}

/// `send` to a daemon's socket, abandoned after `app.daemon_write_timeout`.
/// Either failure is a failed write, and the caller drops the connection.
///
/// A timeout is logged and counted; a socket error is not. A socket that errors
/// is one that closed, which says nothing an operator needs. A write that
/// stalls is a peer that stopped reading, and without this line the router
/// would drop it with nothing to say why — that machine then reads as
/// unreachable, exactly like a closed laptop.
async fn send_within<D>(
    socket: &mut WebSocket,
    app: &App<D>,
    host: &sealmesh_wire::HostId,
    frame: &ServerFrame,
) -> Result<(), ()> {
    match tokio::time::timeout(app.daemon_write_timeout, send(socket, frame)).await {
        Ok(sent) => sent,
        Err(_elapsed) => {
            app.stalled_writes.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "sealmesh-server: dropped {host}'s connection — a write to it took longer than \
                 {}ms, so it has stopped reading",
                app.daemon_write_timeout.as_millis()
            );
            Err(())
        }
    }
}

async fn send_all<D>(
    socket: &mut WebSocket,
    app: &App<D>,
    host: &sealmesh_wire::HostId,
    frames: &[ServerFrame],
) -> Result<(), ()> {
    for frame in frames {
        send_within(socket, app, host, frame).await?;
    }
    Ok(())
}

/// Why a receive did not produce a frame. `Undecodable` and `Closed` were one
/// value until a daemon a version ahead could tear its own connection down by
/// sending a frame this build could not read — and from the reader's side that
/// machine then looked exactly like a closed laptop, with nothing logged.
enum Received<T> {
    Frame(T),
    Undecodable,
    Closed,
}

fn classify<T: for<'de> serde::Deserialize<'de>>(text: &[u8]) -> Received<T> {
    match decode(text) {
        Ok(frame) => Received::Frame(frame),
        Err(_) => Received::Undecodable,
    }
}

async fn recv<T: for<'de> serde::Deserialize<'de>>(socket: &mut WebSocket) -> Received<T> {
    loop {
        let Some(message) = socket.recv().await else {
            return Received::Closed;
        };
        match message {
            Ok(Message::Text(text)) => return classify(text.as_bytes()),
            Ok(Message::Ping(_) | Message::Pong(_)) => continue,
            Ok(Message::Binary(_) | Message::Close(_)) | Err(_) => return Received::Closed,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::{auth::RepoAccess, history::Fake};
    use sealmesh_wire::RepoSlug;

    /// A fresh in-memory history store. Every test in this module posts
    /// through one, so none of them touches AWS.
    fn no_aws() -> Arc<dyn RoomHistory> {
        Arc::new(Fake::new())
    }

    struct Stub {
        login: &'static str,
        member: bool,
    }

    impl Directory for Stub {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Ok(self.login.to_owned())
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(self.member)
        }
        async fn can_read(&self, _token: &SecretToken, _repo: &RepoSlug) -> RepoAccess {
            RepoAccess::Unverifiable
        }
    }

    fn app() -> App<Stub> {
        App::new(
            Stub {
                login: "sandbank",
                member: true,
            },
            ProjectMap::empty(),
            no_aws(),
        )
    }

    #[tokio::test]
    async fn a_reader_is_not_registered_as_a_host() {
        let app = app();
        assert!(app.registry.known_hosts().is_empty());
    }

    #[tokio::test]
    async fn the_host_is_resolved_rather_than_declared() {
        let app = app();
        let host = authenticate(&app.directory, &SecretToken::new("ghp_x".to_owned()))
            .await
            .unwrap();
        assert_eq!(host.as_str(), "sandbank");
    }

    #[tokio::test]
    async fn a_handshake_that_never_arrives_is_abandoned_rather_than_awaited() {
        let started = std::time::Instant::now();
        let never = std::future::pending::<Option<()>>();
        assert_eq!(within(Duration::from_millis(50), never).await, None);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the deadline did not fire: waited {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn a_handshake_that_arrives_in_time_is_kept() {
        assert_eq!(
            within(Duration::from_secs(30), async { Some(7) }).await,
            Some(7)
        );
    }

    #[test]
    fn the_handshake_deadline_is_bounded() {
        assert!(
            HANDSHAKE_TIMEOUT <= Duration::from_secs(30),
            "{HANDSHAKE_TIMEOUT:?}"
        );
    }

    #[tokio::test]
    async fn a_token_from_outside_the_org_cannot_connect() {
        let app = App::new(
            Stub {
                login: "outsider",
                member: false,
            },
            ProjectMap::empty(),
            no_aws(),
        );
        assert_eq!(
            authenticate(&app.directory, &SecretToken::new("ghp_x".to_owned())).await,
            Err(Denied::NotAuthorised)
        );
    }

    const READER_TOKEN: &str = "ghp_reader";

    /// Grants by exact slug, and only to the reader's own token.
    struct Access {
        readable: &'static [&'static str],
        unverifiable: &'static [&'static str],
    }

    impl Directory for Access {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Ok("reader".to_owned())
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(true)
        }
        async fn can_read(&self, token: &SecretToken, repo: &RepoSlug) -> RepoAccess {
            if token.expose() != READER_TOKEN {
                RepoAccess::NotReadable
            } else if self.readable.contains(&repo.as_str()) {
                RepoAccess::Readable
            } else if self.unverifiable.contains(&repo.as_str()) {
                RepoAccess::Unverifiable
            } else {
                RepoAccess::NotReadable
            }
        }
    }

    fn host(name: &str) -> sealmesh_wire::HostId {
        sealmesh_wire::HostId::try_from(name).unwrap()
    }

    fn reader_token() -> SecretToken {
        SecretToken::new(READER_TOKEN.to_owned())
    }

    fn answering(snapshot_host: &str, repos: &[&str]) -> sealmesh_proto::HostState {
        let agents: Vec<String> = repos
            .iter()
            .enumerate()
            .map(|(i, repo)| {
                format!(
                    r#"{{"id":"{:08x}","repo":"{repo}","run_state":"working",
                        "started_at":"2026-08-10T19:44:51.322Z",
                        "updated_at":"2026-08-10T19:56:51.322Z"}}"#,
                    i + 1
                )
            })
            .collect();
        let json = format!(
            r#"{{"schema":2,"host":"{snapshot_host}",
                 "collected_at":"2026-08-10T19:56:51.322Z","agents":[{}]}}"#,
            agents.join(",")
        );
        sealmesh_proto::HostState::Answering {
            snapshot: Box::new(sealmesh_wire::Snapshot::from_json(json.as_bytes()).unwrap()),
        }
    }

    fn report_repos(frames: &[ServerFrame]) -> Vec<String> {
        frames
            .iter()
            .flat_map(|frame| match frame {
                ServerFrame::Report {
                    state: sealmesh_proto::HostState::Answering { snapshot },
                    ..
                } => snapshot
                    .agents
                    .iter()
                    .map(|agent| agent.repo.as_str().to_owned())
                    .collect(),
                _ => Vec::new(),
            })
            .collect()
    }

    fn kinds(frames: &[ServerFrame], id: QueryId) -> Vec<&'static str> {
        frames
            .iter()
            .map(|frame| match frame {
                ServerFrame::Report { id: of, .. } if *of == id => "report",
                ServerFrame::Partial { id: of } if *of == id => "partial",
                ServerFrame::Complete { id: of } if *of == id => "complete",
                _ => "unexpected",
            })
            .collect()
    }

    /// Runs `answer_ask` against one attached daemon that answers with
    /// `repos`, the way `serve_reader` does for a real `Ask`.
    async fn ask_through_the_reader_path(app: &App<Access>, repos: &[&str]) -> Vec<ServerFrame> {
        let (mut rx, _guard) = app.registry.attach(host("sandbank"));
        let daemon = async {
            let Some(ServerFrame::Query { id, .. }) = rx.recv().await else {
                panic!("the daemon was never asked");
            };
            app.registry.resolve(id, answering("sandbank", repos));
        };
        let reader = host("reader");
        let token = reader_token();
        let (frames, ()) = tokio::join!(answer_ask(app, &reader, &token, QueryId(3)), daemon);
        frames
    }

    #[tokio::test]
    async fn an_ask_on_the_reader_path_withholds_unreadable_and_unverifiable_rows() {
        let app = App::new(
            Access {
                readable: &["example-org/sealmesh"],
                unverifiable: &["example-org/renamed"],
            },
            ProjectMap::empty(),
            no_aws(),
        );
        let frames = ask_through_the_reader_path(
            &app,
            &[
                "example-org/sealmesh",
                "example-org/private",
                "example-org/renamed",
            ],
        )
        .await;

        assert_eq!(report_repos(&frames), vec!["example-org/sealmesh"]);
        assert_eq!(
            kinds(&frames, QueryId(3)),
            vec!["report", "partial", "complete"]
        );
    }

    #[tokio::test]
    async fn an_ask_from_a_reader_who_can_read_everything_is_whole_and_not_partial() {
        let app = App::new(
            Access {
                readable: &["example-org/sealmesh", "example-org/example-repo"],
                unverifiable: &[],
            },
            ProjectMap::empty(),
            no_aws(),
        );
        let frames = ask_through_the_reader_path(
            &app,
            &["example-org/sealmesh", "example-org/example-repo"],
        )
        .await;

        assert_eq!(
            report_repos(&frames),
            vec!["example-org/sealmesh", "example-org/example-repo"]
        );
        assert_eq!(kinds(&frames, QueryId(3)), vec!["report", "complete"]);
    }

    #[tokio::test]
    async fn every_ask_on_the_reader_path_draws_on_one_allowance_for_that_reader() {
        use crate::{
            check_budget::{CHECK_BURST, ChecksPerHour},
            query::MAX_REPOS_PER_QUERY,
        };

        let covered = usize::try_from(CHECK_BURST).unwrap() / MAX_REPOS_PER_QUERY;
        let names: Vec<&'static str> = (0..MAX_REPOS_PER_QUERY * (covered + 1))
            .map(|i| &*Box::leak(format!("example-org/r{i:03}").into_boxed_str()))
            .collect();
        let readable: &'static [&'static str] = Box::leak(names.into_boxed_slice());
        let mut app = App::new(
            Access {
                readable,
                unverifiable: &[],
            },
            ProjectMap::empty(),
            no_aws(),
        );
        app.check_budgets = CheckBudgets::new(ChecksPerHour::MIN);

        for (ask, repos) in readable.chunks(MAX_REPOS_PER_QUERY).enumerate() {
            let frames = ask_through_the_reader_path(&app, repos).await;
            if ask < covered {
                assert_eq!(
                    report_repos(&frames).len(),
                    MAX_REPOS_PER_QUERY,
                    "ask {ask}"
                );
                assert_eq!(kinds(&frames, QueryId(3)), vec!["report", "complete"]);
            } else {
                assert!(report_repos(&frames).is_empty(), "ask {ask}");
                assert_eq!(
                    kinds(&frames, QueryId(3)),
                    vec!["report", "partial", "complete"]
                );
            }
        }
    }

    #[tokio::test]
    async fn a_projects_request_on_the_reader_path_omits_hidden_projects_and_repos() {
        let map = ProjectMap::parse(
            r#"[{"id":"example-repo","repos":["example-org/example-repo","example-org/example-repo-portal"]},
                {"id":"secret","repos":["example-org/secret"]}]"#,
        )
        .unwrap();
        let app = App::new(
            Access {
                readable: &["example-org/example-repo"],
                unverifiable: &["example-org/example-repo-portal"],
            },
            map,
            no_aws(),
        );

        let reply = answer_projects(&app, &host("reader"), &reader_token()).await;

        assert_eq!(
            reply,
            ServerFrame::Projects {
                projects: vec![sealmesh_proto::ProjectEntry {
                    id: sealmesh_wire::ProjectId::try_from("example-repo").unwrap(),
                    repos: vec![RepoSlug::try_from("example-org/example-repo").unwrap()],
                }],
            }
        );
    }

    fn not_sharing(host: &str) -> (sealmesh_wire::HostId, sealmesh_proto::HostState) {
        (
            sealmesh_wire::HostId::try_from(host).unwrap(),
            sealmesh_proto::HostState::NotSharing {},
        )
    }

    #[test]
    fn a_partial_answer_says_so_after_the_reports_and_before_complete() {
        // An installed reader stops listening at `Complete`. A `Partial` sent
        // after it would reach nobody who could skip it; one sent before it is
        // skipped as undecodable and the reader still completes.
        let id = QueryId(7);
        let frames = answer_frames(
            id,
            Narrowed {
                reports: vec![not_sharing("sandbank"), not_sharing("shelter")],
                partial: true,
            },
        );

        let kinds: Vec<&str> = frames
            .iter()
            .map(|frame| match frame {
                ServerFrame::Report { id: of, .. } if *of == id => "report",
                ServerFrame::Partial { id: of } if *of == id => "partial",
                ServerFrame::Complete { id: of } if *of == id => "complete",
                _ => "unexpected",
            })
            .collect();
        assert_eq!(kinds, vec!["report", "report", "partial", "complete"]);
    }

    #[test]
    fn a_complete_answer_carries_no_partial_frame() {
        let (host, state) = not_sharing("sandbank");
        let frames = answer_frames(
            QueryId(7),
            Narrowed {
                reports: vec![(host.clone(), state.clone())],
                partial: false,
            },
        );
        assert_eq!(
            frames,
            vec![
                ServerFrame::Report {
                    id: QueryId(7),
                    host,
                    state
                },
                ServerFrame::Complete { id: QueryId(7) },
            ]
        );
    }

    #[test]
    fn a_partial_answer_with_no_machines_still_completes() {
        assert_eq!(
            answer_frames(
                QueryId(7),
                Narrowed {
                    reports: vec![],
                    partial: true
                }
            ),
            vec![
                ServerFrame::Partial { id: QueryId(7) },
                ServerFrame::Complete { id: QueryId(7) },
            ]
        );
    }

    #[test]
    fn a_frame_the_router_cannot_read_is_not_a_closed_socket() {
        // These two were one value. Conflating them let a daemon one version
        // ahead tear down its own connection, and the reader then saw that
        // machine as unreachable — identical to a closed laptop, with nothing
        // logged to say otherwise.
        for hostile in [
            br#"{"kind":"answer","id":1,"state":{"kind":"answering"}}"#.as_slice(),
            br#"{"kind":"somethingNewer"}"#,
            b"not json at all",
        ] {
            assert!(matches!(
                classify::<ClientFrame>(hostile),
                Received::Undecodable
            ));
        }

        let good = serde_json::to_string(&ClientFrame::Ask {
            id: sealmesh_proto::QueryId(3),
        })
        .unwrap();
        assert!(matches!(
            classify::<ClientFrame>(good.as_bytes()),
            Received::Frame(ClientFrame::Ask { .. })
        ));
    }

    // Rooms, end to end: a real `/v1/connect` socket per daemon, so the
    // handshake, the frame cap and the undecodable-frame handling are the
    // production path rather than a function called around it.

    use futures_util::{SinkExt as _, StreamExt as _};
    use sealmesh_chat_wire::{Body, MessageKind, RoomId, Scope};
    use sealmesh_proto::RoomSeq;
    use tokio_tungstenite::tungstenite::Message as WsMessage;

    type Ws = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;

    const ROOM_MAP: &str = r#"[
        {"id":"example-repo","repos":["example-org/example-repo","example-org/example-repo-portal"]},
        {"id":"example-tools","repos":["example-org/example-tools"]}
    ]"#;
    const EXAMPLE_REPO: &str = "example-org/example-repo";
    const SKILLS: &str = "example-org/example-tools";
    /// In no project in `ROOM_MAP`.
    const UNMAPPED: &str = "example-org/sealmesh";

    /// An org where the token `ghp_<login>` is `<login>`, and read access is
    /// exact `(token, repo)` grants that a test can revoke. `hang` makes every
    /// access check wait forever, the way a GitHub that never answers would.
    struct Org {
        grants: std::sync::Mutex<Vec<(String, String)>>,
        hang: std::sync::atomic::AtomicBool,
        /// What every repo's pull request listing answers. Empty by default:
        /// GitHub answered, and nothing has merged.
        pulls:
            std::sync::Mutex<Result<Vec<crate::auth::PullSummary>, crate::auth::PullsUnverifiable>>,
        /// Listings started, counted before `pulls_hang` is honoured, so a
        /// test can wait until a merge check is provably in flight.
        pulls_started: std::sync::atomic::AtomicUsize,
        /// Makes every listing wait for as long as it is set.
        pulls_hang: std::sync::atomic::AtomicBool,
        /// Access checks started, counted before `hang` is honoured, so a
        /// test can wait until a check is provably in flight.
        reads_started: std::sync::atomic::AtomicUsize,
    }

    impl Org {
        fn revoke(&self, token: &str, repo: &str) {
            self.grants
                .lock()
                .unwrap()
                .retain(|(t, r)| !(t == token && r == repo));
        }
    }

    impl Directory for Org {
        async fn login(&self, token: &SecretToken) -> Result<String, Denied> {
            token
                .expose()
                .strip_prefix("ghp_")
                .map(str::to_owned)
                .ok_or(Denied::NotAuthorised)
        }
        async fn is_member(&self, token: &SecretToken) -> Result<bool, Denied> {
            Ok(token.expose().starts_with("ghp_"))
        }
        async fn can_read(&self, token: &SecretToken, repo: &RepoSlug) -> RepoAccess {
            // Waits for as long as `hang` is set, so a test can hold a check
            // open and later let it finish — which is how a cancelled check is
            // told apart from one that was merely still running.
            self.reads_started
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            while self.hang.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
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
        async fn recent_pulls(
            &self,
            _token: &SecretToken,
            _repo: &RepoSlug,
        ) -> Result<Vec<crate::auth::PullSummary>, crate::auth::PullsUnverifiable> {
            self.pulls_started
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            while self.pulls_hang.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            self.pulls.lock().unwrap().clone()
        }
    }

    fn org_app(grants: &[(&str, &str)]) -> App<Org> {
        App::new(
            Org {
                grants: std::sync::Mutex::new(
                    grants
                        .iter()
                        .map(|(t, r)| ((*t).to_owned(), (*r).to_owned()))
                        .collect(),
                ),
                hang: std::sync::atomic::AtomicBool::new(false),
                pulls: std::sync::Mutex::new(Ok(Vec::new())),
                pulls_started: std::sync::atomic::AtomicUsize::new(0),
                pulls_hang: std::sync::atomic::AtomicBool::new(false),
                reads_started: std::sync::atomic::AtomicUsize::new(0),
            },
            ProjectMap::parse(ROOM_MAP).unwrap(),
            no_aws(),
        )
    }

    /// Asks every attached daemon, the way a reader's `Ask` does, while
    /// `daemon` answers its `Query` — and requires the answer inside
    /// `FANOUT_TIMEOUT`. A router whose connection loop is stuck awaiting
    /// something else never writes the `Query` to the socket, and the machine
    /// reads as unreachable to every `who`.
    async fn answers_a_query_within_the_fanout_timeout(
        app: &App<Org>,
        daemon: &mut Ws,
        login: &str,
    ) {
        let reader = host("reader");
        let started = std::time::Instant::now();
        let answering = async {
            match next_frame(daemon).await {
                Some(ServerFrame::Query { id, .. }) => {
                    send_frame(
                        daemon,
                        &ClientFrame::Answer {
                            id,
                            state: sealmesh_proto::HostState::NotSharing {},
                        },
                    )
                    .await;
                }
                other => panic!("expected a query, got {other:?}"),
            }
        };
        let (report, ()) = tokio::join!(
            crate::query::ask_all(&app.registry, &reader, FANOUT_TIMEOUT),
            answering
        );
        assert_eq!(
            report,
            vec![(host(login), sealmesh_proto::HostState::NotSharing {})],
            "the daemon read as unreachable while an access check was in flight"
        );
        assert!(
            started.elapsed() < FANOUT_TIMEOUT,
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn a_daemon_mid_join_on_a_resolver_that_never_answers_still_answers_a_query() {
        let app = Arc::new(org_app(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_atlas", SKILLS),
        ]));
        app.directory
            .hang
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let addr = serve(Arc::clone(&app)).await;
        let mut atlas = daemon(addr, "atlas").await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);

        send_frame(
            &mut atlas,
            &ClientFrame::Join {
                room: example_repo.clone(),
            },
        )
        .await;
        // Long enough for the router to have started a check that never ends.
        tokio::time::sleep(Duration::from_millis(100)).await;

        answers_a_query_within_the_fanout_timeout(&app, &mut atlas, "atlas").await;

        // One check at a time per connection: a second join while the first
        // is still running is refused straight away, not queued behind it.
        let second = repo_room("example-tools", SKILLS);
        assert_eq!(
            join(&mut atlas, &second).await,
            Some(ServerFrame::JoinRefused { room: second })
        );
        assert_eq!(app.rooms.member_count(&example_repo), 0);
    }

    #[tokio::test]
    async fn over_the_socket_a_room_that_re_forms_never_resends_what_was_queued_before_the_left() {
        // The reviewer's probe: bay, alone in a room, pipelines two posts, a
        // leave and a rejoin in one burst. His leave empties the room, so the
        // rejoin re-forms it. A copy of either post that arrives after his
        // `Left` came from the room's previous life. One round leaked about one
        // time in six, so this runs forty-five of them across three
        // connections, each within one connection's join rate limit.
        // Unmetered: this probes delivery order with more posts than the post
        // rate allows, and the rate limit is not what it tests.
        let mut app = org_app(&[("ghp_bay", EXAMPLE_REPO)]);
        app.rooms = Rooms::unmetered();
        let app = Arc::new(app);
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);

        for connection in 0..3 {
            let mut bay = daemon(addr, "bay").await;
            assert_eq!(
                join(&mut bay, &example_repo).await,
                Some(ServerFrame::Joined {
                    room: example_repo.clone()
                })
            );
            for round in 0..15 {
                let old = [
                    format!("c{connection}r{round}-old1"),
                    format!("c{connection}r{round}-old2"),
                ];
                for text in &old {
                    send_frame(&mut bay, &post(&example_repo, text)).await;
                }
                send_frame(
                    &mut bay,
                    &ClientFrame::Leave {
                        room: example_repo.clone(),
                    },
                )
                .await;
                send_frame(
                    &mut bay,
                    &ClientFrame::Join {
                        room: example_repo.clone(),
                    },
                )
                .await;

                let mut left = false;
                loop {
                    match next_frame(&mut bay).await {
                        Some(ServerFrame::Message { body, .. }) => {
                            assert!(
                                !left || !old.iter().any(|text| text == body.as_str()),
                                "{} arrived after bay's Left",
                                body.as_str()
                            );
                        }
                        Some(ServerFrame::Left { .. }) => left = true,
                        Some(ServerFrame::Joined { .. }) => {
                            assert!(left, "Joined arrived before Left");
                            break;
                        }
                        other => panic!("round {round}: unexpected {other:?}"),
                    }
                }

                // Anything still queued from before the Left would reach bay
                // ahead of this.
                let sentinel = format!("c{connection}r{round}-sentinel");
                send_frame(&mut bay, &post(&example_repo, &sentinel)).await;
                match next_frame(&mut bay).await {
                    Some(ServerFrame::Message { body, .. }) if body.as_str() == sentinel => {}
                    Some(ServerFrame::Message { body, .. }) => panic!(
                        "{} from the room's previous life arrived after the rejoin",
                        body.as_str()
                    ),
                    other => panic!("round {round}: unexpected {other:?}"),
                }
            }
        }
    }

    #[tokio::test]
    async fn a_repeat_join_of_a_room_already_held_answers_joined_without_asking_github() {
        let app = Arc::new(org_app(&[("ghp_atlas", EXAMPLE_REPO)]));
        let addr = serve(Arc::clone(&app)).await;
        let mut atlas = daemon(addr, "atlas").await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        assert_eq!(
            join(&mut atlas, &example_repo).await,
            Some(ServerFrame::Joined {
                room: example_repo.clone()
            })
        );

        // From here every access check waits forever, so an answer can only
        // come from not asking GitHub at all.
        app.directory
            .hang
            .store(true, std::sync::atomic::Ordering::SeqCst);
        for spelling in [
            example_repo.clone(),
            repo_room("example-repo", "Example-org/Example-repo"),
        ] {
            assert_eq!(
                join(&mut atlas, &spelling).await,
                Some(ServerFrame::Joined {
                    room: example_repo.clone()
                }),
                "a repeat join of a room already held waited on an access check"
            );
        }
        assert_eq!(app.rooms.member_count(&example_repo), 1);
    }

    #[tokio::test]
    async fn a_repeat_join_then_leave_answers_joined_then_left_and_leaves_the_room() {
        let app = Arc::new(org_app(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_bay", EXAMPLE_REPO),
        ]));
        let addr = serve(Arc::clone(&app)).await;
        let mut atlas = daemon(addr, "atlas").await;
        let mut bay = daemon(addr, "bay").await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        for member in [&mut atlas, &mut bay] {
            assert!(matches!(
                join(member, &example_repo).await,
                Some(ServerFrame::Joined { .. })
            ));
        }

        // The reviewer's probe: bay, already in the room, asks to join it again
        // and leaves straight away, while any access check would still be
        // running.
        app.directory
            .hang
            .store(true, std::sync::atomic::Ordering::SeqCst);
        send_frame(
            &mut bay,
            &ClientFrame::Join {
                room: example_repo.clone(),
            },
        )
        .await;
        send_frame(
            &mut bay,
            &ClientFrame::Leave {
                room: example_repo.clone(),
            },
        )
        .await;

        assert_eq!(
            next_frame(&mut bay).await,
            Some(ServerFrame::Joined {
                room: example_repo.clone()
            })
        );
        assert_eq!(
            next_frame(&mut bay).await,
            Some(ServerFrame::Left {
                room: example_repo.clone()
            })
        );
        assert_eq!(
            app.rooms.member_count(&example_repo),
            1,
            "bay's connection still holds the room after its Left"
        );

        send_frame(&mut atlas, &post(&example_repo, "after bay left")).await;
        assert_eq!(message_body(next_frame(&mut atlas).await), "after bay left");
        assert!(
            quiet_for(&mut bay, Duration::from_millis(300)).await,
            "a message reached bay after his Left"
        );
    }

    #[tokio::test]
    async fn a_leave_during_an_in_flight_join_cancels_it_and_the_join_never_admits() {
        let app = Arc::new(org_app(&[("ghp_atlas", EXAMPLE_REPO)]));
        app.directory
            .hang
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let addr = serve(Arc::clone(&app)).await;
        let mut atlas = daemon(addr, "atlas").await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);

        send_frame(
            &mut atlas,
            &ClientFrame::Join {
                room: example_repo.clone(),
            },
        )
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Spelled differently from the join, so the match has to be on the
        // room's one key rather than on the bytes the daemon sent.
        send_frame(
            &mut atlas,
            &ClientFrame::Leave {
                room: repo_room("example-repo", "Example-org/Example-repo"),
            },
        )
        .await;
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::JoinRefused {
                room: example_repo.clone()
            }),
            "a leave during the join was not answered as a refused join"
        );

        // Let the resolver answer. A join that was only abandoned, rather than
        // cancelled, would now admit atlas and send `Joined`.
        app.directory
            .hang
            .store(false, std::sync::atomic::Ordering::SeqCst);
        assert!(
            quiet_for(&mut atlas, Duration::from_millis(300)).await,
            "the cancelled join still answered"
        );
        assert_eq!(app.rooms.member_count(&example_repo), 0);

        // The check slot is free again: a fresh join is checked and granted,
        // not refused as busy.
        assert_eq!(
            join(&mut atlas, &example_repo).await,
            Some(ServerFrame::Joined { room: example_repo })
        );
    }

    #[tokio::test]
    async fn a_recheck_that_falls_due_during_a_join_runs_as_soon_as_the_join_finishes() {
        // A two-second period: with jitter, the first re-check falls due
        // between two and three seconds after connecting, and the next one no
        // sooner than four. Holding a join open past three seconds therefore
        // makes the first re-check fall due while the join is running.
        let mut app = org_app(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_atlas", SKILLS)]);
        app.room_revalidate_every = Duration::from_secs(2);
        let app = Arc::new(app);
        let addr = serve(Arc::clone(&app)).await;
        let mut atlas = daemon(addr, "atlas").await;
        let connected = std::time::Instant::now();
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let skills = repo_room("example-tools", SKILLS);
        assert_eq!(
            join(&mut atlas, &example_repo).await,
            Some(ServerFrame::Joined {
                room: example_repo.clone()
            })
        );

        app.directory
            .hang
            .store(true, std::sync::atomic::Ordering::SeqCst);
        send_frame(
            &mut atlas,
            &ClientFrame::Join {
                room: skills.clone(),
            },
        )
        .await;
        tokio::time::sleep(Duration::from_millis(3_300).saturating_sub(connected.elapsed())).await;

        // atlas loses example-repo while its re-check waits behind the join, and
        // then the join is let through.
        app.directory.revoke("ghp_atlas", EXAMPLE_REPO);
        app.directory
            .hang
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let released = std::time::Instant::now();

        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::Joined { room: skills })
        );
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::Left {
                room: example_repo.clone()
            })
        );
        assert!(
            released.elapsed() < Duration::from_millis(600),
            "the re-check that fell due during the join waited for the next period: {:?}",
            released.elapsed()
        );
        assert_eq!(app.rooms.member_count(&example_repo), 0);
    }

    #[tokio::test]
    async fn a_daemon_mid_recheck_on_a_resolver_that_never_answers_still_answers_a_query() {
        let mut app = org_app(&[("ghp_atlas", EXAMPLE_REPO)]);
        app.room_revalidate_every = Duration::from_millis(100);
        let app = Arc::new(app);
        let addr = serve(Arc::clone(&app)).await;
        let mut atlas = daemon(addr, "atlas").await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        assert_eq!(
            join(&mut atlas, &example_repo).await,
            Some(ServerFrame::Joined {
                room: example_repo.clone()
            })
        );

        app.directory
            .hang
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // Several re-check periods, so one has started and is stuck.
        tokio::time::sleep(Duration::from_millis(400)).await;

        answers_a_query_within_the_fanout_timeout(&app, &mut atlas, "atlas").await;
        assert_eq!(
            app.rooms.member_count(&example_repo),
            1,
            "a re-check that has not finished removed the member"
        );
    }

    async fn serve(app: Arc<App<Org>>) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router(app)).await.unwrap();
        });
        addr
    }

    async fn open(addr: std::net::SocketAddr) -> Ws {
        tokio_tungstenite::connect_async(format!("ws://{addr}/v1/connect"))
            .await
            .unwrap()
            .0
    }

    async fn send_text(ws: &mut Ws, text: String) {
        ws.send(WsMessage::Text(text.into())).await.unwrap();
    }

    async fn send_frame(ws: &mut Ws, frame: &ClientFrame) {
        send_text(ws, serde_json::to_string(frame).unwrap()).await;
    }

    /// The next frame, or `None` once the router has closed the socket.
    async fn read_frame(ws: &mut Ws) -> Option<ServerFrame> {
        loop {
            match ws.next().await {
                Some(Ok(WsMessage::Text(text))) => {
                    return Some(serde_json::from_str(text.as_str()).unwrap());
                }
                Some(Ok(WsMessage::Ping(_) | WsMessage::Pong(_))) => continue,
                _ => return None,
            }
        }
    }

    async fn next_frame(ws: &mut Ws) -> Option<ServerFrame> {
        tokio::time::timeout(Duration::from_secs(5), read_frame(ws))
            .await
            .expect("the router neither sent a frame nor closed the socket within 5s")
    }

    /// Whether nothing at all arrives within `wait`.
    async fn quiet_for(ws: &mut Ws, wait: Duration) -> bool {
        tokio::time::timeout(wait, read_frame(ws)).await.is_err()
    }

    async fn daemon(addr: std::net::SocketAddr, login: &str) -> Ws {
        let mut ws = open(addr).await;
        send_frame(
            &mut ws,
            &ClientFrame::Hello {
                role: Role::Daemon,
                token: SecretToken::new(format!("ghp_{login}")),
            },
        )
        .await;
        assert!(matches!(
            next_frame(&mut ws).await,
            Some(ServerFrame::Welcome { .. })
        ));
        ws
    }

    async fn join(ws: &mut Ws, room: &RoomId) -> Option<ServerFrame> {
        send_frame(ws, &ClientFrame::Join { room: room.clone() }).await;
        next_frame(ws).await
    }

    fn repo_room(project: &str, slug: &str) -> RoomId {
        RoomId::new(
            sealmesh_wire::ProjectId::try_from(project).unwrap(),
            Scope::Repo(RepoSlug::try_from(slug).unwrap()),
        )
    }

    fn post(room: &RoomId, text: &str) -> ClientFrame {
        ClientFrame::Post {
            room: room.clone(),
            kind: MessageKind::Claim,
            body: Body::new(text).unwrap(),
        }
    }

    fn message_body(frame: Option<ServerFrame>) -> String {
        match frame {
            Some(ServerFrame::Message { body, .. }) => body.as_str().to_owned(),
            other => panic!("expected a room message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn over_the_socket_a_post_reaches_every_member_and_no_one_else() {
        let app = Arc::new(org_app(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_atlas", SKILLS),
            ("ghp_bay", EXAMPLE_REPO),
            ("ghp_cedar", SKILLS),
            ("ghp_uplink", UNMAPPED),
        ]));
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let skills = repo_room("example-tools", SKILLS);

        let mut atlas = daemon(addr, "atlas").await;
        let mut bay = daemon(addr, "bay").await;
        let mut cedar = daemon(addr, "cedar").await;
        let mut uplink = daemon(addr, "uplink").await;

        let joined = |room: &RoomId| Some(ServerFrame::Joined { room: room.clone() });
        let refused = |room: &RoomId| Some(ServerFrame::JoinRefused { room: room.clone() });
        assert_eq!(join(&mut atlas, &example_repo).await, joined(&example_repo));
        assert_eq!(join(&mut atlas, &skills).await, joined(&skills));
        assert_eq!(join(&mut bay, &example_repo).await, joined(&example_repo));
        assert_eq!(join(&mut cedar, &skills).await, joined(&skills));
        assert_eq!(
            join(&mut cedar, &example_repo).await,
            refused(&example_repo)
        );
        // A repo in no project gets no room, under any project it names.
        for attempt in [
            repo_room("example-repo", UNMAPPED),
            repo_room("sealmesh", UNMAPPED),
            RoomId::new(
                sealmesh_wire::ProjectId::try_from("sealmesh").unwrap(),
                Scope::Project,
            ),
        ] {
            assert_eq!(join(&mut uplink, &attempt).await, refused(&attempt));
        }

        send_frame(&mut atlas, &post(&example_repo, "taking the portal")).await;
        // atlas's own copy is queued in the same step as every other member's,
        // so once it arrives anything meant for anyone else is already sent.
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::Message {
                room: example_repo.clone(),
                seq: RoomSeq(1),
                from: host("atlas"),
                kind: MessageKind::Claim,
                body: Body::new("taking the portal").unwrap(),
            })
        );
        assert_eq!(
            message_body(next_frame(&mut bay).await),
            "taking the portal"
        );

        // A sentinel in the room cedar is in. Had the first message reached
        // her, it would be ahead of this one on her socket.
        send_frame(&mut atlas, &post(&skills, "sentinel")).await;
        assert_eq!(message_body(next_frame(&mut atlas).await), "sentinel");
        assert_eq!(message_body(next_frame(&mut cedar).await), "sentinel");
        assert!(
            quiet_for(&mut uplink, Duration::from_millis(300)).await,
            "a daemon whose repo is in no project received a room message"
        );
        assert!(
            quiet_for(&mut bay, Duration::from_millis(100)).await,
            "bay received a message from a room he is not in"
        );
        assert_eq!(app.rooms.room_count(), 2);
    }

    #[tokio::test]
    async fn a_room_frame_before_the_handshake_is_denied_and_acts_on_nothing() {
        let app = Arc::new(org_app(&[("ghp_atlas", EXAMPLE_REPO)]));
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        join(&mut atlas, &example_repo).await;

        for first in [
            ClientFrame::Join {
                room: example_repo.clone(),
            },
            post(&example_repo, "before hello"),
        ] {
            let mut stranger = open(addr).await;
            send_frame(&mut stranger, &first).await;
            assert_eq!(
                next_frame(&mut stranger).await,
                Some(ServerFrame::Denied {})
            );
            assert_eq!(
                next_frame(&mut stranger).await,
                None,
                "the socket stayed open"
            );
        }

        assert!(
            quiet_for(&mut atlas, Duration::from_millis(200)).await,
            "an unauthenticated post was delivered"
        );
        assert_eq!(app.rooms.connection_count(), 1);
        assert_eq!(app.rooms.member_count(&example_repo), 1);
    }

    #[tokio::test]
    async fn forged_room_and_sequence_frames_are_dropped_and_the_socket_stays_open() {
        let app = Arc::new(org_app(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_elm", EXAMPLE_REPO),
        ]));
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        let mut elm = daemon(addr, "elm").await;
        join(&mut atlas, &example_repo).await;

        // elm could join, but has not: her post and leave name a room she is
        // not in.
        send_frame(&mut elm, &post(&example_repo, "forged room")).await;
        send_frame(
            &mut elm,
            &ClientFrame::Leave {
                room: example_repo.clone(),
            },
        )
        .await;

        // From a member: a post naming its own sequence number, one naming its
        // own sender, and a router `message` sent as if a client could.
        let room = serde_json::to_value(&example_repo).unwrap();
        for hostile in [
            serde_json::json!({"type":"post","room":room,"kind":"claim","body":"forged seq","seq":1}),
            serde_json::json!({"type":"post","room":room,"kind":"claim","body":"forged sender","from":"bay"}),
            serde_json::json!({"type":"message","room":room,"seq":1,"from":"bay","kind":"claim","body":"forged message"}),
        ] {
            send_text(&mut atlas, hostile.to_string()).await;
        }

        // atlas's socket is still open, and the first message anyone sees is
        // her real one, with the router's first sequence number.
        send_frame(&mut atlas, &post(&example_repo, "real")).await;
        match next_frame(&mut atlas).await {
            Some(ServerFrame::Message {
                seq, from, body, ..
            }) => {
                assert_eq!(seq, RoomSeq(1));
                assert_eq!(from, host("atlas"));
                assert_eq!(body.as_str(), "real");
            }
            other => panic!("{other:?}"),
        }
        assert!(quiet_for(&mut elm, Duration::from_millis(200)).await);

        // elm's socket is still open too, and she can join properly.
        assert_eq!(
            join(&mut elm, &example_repo).await,
            Some(ServerFrame::Joined {
                room: example_repo.clone()
            })
        );
        send_frame(&mut atlas, &post(&example_repo, "after")).await;
        assert_eq!(message_body(next_frame(&mut atlas).await), "after");
        match next_frame(&mut elm).await {
            Some(ServerFrame::Message { seq, body, .. }) => {
                assert_eq!(seq, RoomSeq(2), "a dropped frame spent a sequence number");
                assert_eq!(body.as_str(), "after");
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn an_oversized_frame_is_refused_at_the_transport_and_takes_its_member_out() {
        let app = Arc::new(org_app(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_bay", EXAMPLE_REPO),
        ]));
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        let mut bay = daemon(addr, "bay").await;
        join(&mut atlas, &example_repo).await;
        join(&mut bay, &example_repo).await;

        let oversized = format!(
            r#"{{"type":"post","room":{},"kind":"claim","body":"{}"}}"#,
            serde_json::to_string(&example_repo).unwrap(),
            "x".repeat(MAX_FRAME_BYTES)
        );
        // The router may close before the write finishes; that is the point.
        let _ = bay.send(WsMessage::Text(oversized.into())).await;
        // Refused by the decoder instead, this socket would stay open and
        // this would time out.
        assert_eq!(next_frame(&mut bay).await, None);

        tokio::time::timeout(Duration::from_secs(5), async {
            while app.rooms.member_count(&example_repo) != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the closed connection was never taken out of its room");

        send_frame(&mut atlas, &post(&example_repo, "still here")).await;
        assert_eq!(message_body(next_frame(&mut atlas).await), "still here");
    }

    #[tokio::test]
    async fn a_daemon_whose_access_is_revoked_is_taken_out_of_the_room_at_the_recheck() {
        let mut app = org_app(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        app.room_revalidate_every = Duration::from_millis(100);
        let app = Arc::new(app);
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        let mut bay = daemon(addr, "bay").await;
        join(&mut atlas, &example_repo).await;
        join(&mut bay, &example_repo).await;

        app.directory.revoke("ghp_bay", EXAMPLE_REPO);

        assert_eq!(
            next_frame(&mut bay).await,
            Some(ServerFrame::Left {
                room: example_repo.clone()
            })
        );
        send_frame(&mut atlas, &post(&example_repo, "after revoke")).await;
        assert_eq!(message_body(next_frame(&mut atlas).await), "after revoke");
        assert!(quiet_for(&mut bay, Duration::from_millis(200)).await);
        assert_eq!(app.rooms.member_count(&example_repo), 1);
    }

    #[tokio::test]
    async fn a_reader_cannot_join_a_room() {
        let app = Arc::new(org_app(&[("ghp_atlas", EXAMPLE_REPO)]));
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);

        let mut reader = open(addr).await;
        send_frame(
            &mut reader,
            &ClientFrame::Hello {
                role: Role::Reader,
                token: SecretToken::new("ghp_atlas".to_owned()),
            },
        )
        .await;
        assert!(matches!(
            next_frame(&mut reader).await,
            Some(ServerFrame::Welcome { .. })
        ));
        send_frame(
            &mut reader,
            &ClientFrame::Join {
                room: example_repo.clone(),
            },
        )
        .await;
        assert!(quiet_for(&mut reader, Duration::from_millis(200)).await);
        assert_eq!(app.rooms.connection_count(), 0);

        let mut atlas = daemon(addr, "atlas").await;
        join(&mut atlas, &example_repo).await;
        send_frame(&mut atlas, &post(&example_repo, "daemons only")).await;
        assert_eq!(message_body(next_frame(&mut atlas).await), "daemons only");
        assert!(quiet_for(&mut reader, Duration::from_millis(200)).await);
    }

    #[test]
    fn only_a_hello_opens_a_session() {
        let room = repo_room("example-repo", EXAMPLE_REPO);
        for frame in [
            ClientFrame::Join { room: room.clone() },
            ClientFrame::Leave { room: room.clone() },
            post(&room, "x"),
            ClientFrame::Projects {},
            ClientFrame::Ask { id: QueryId(1) },
        ] {
            assert!(credential(frame).is_none());
        }
        assert!(
            credential(ClientFrame::Hello {
                role: Role::Daemon,
                token: SecretToken::new("ghp_x".to_owned()),
            })
            .is_some()
        );
    }

    #[tokio::test]
    async fn a_recheck_period_out_of_bounds_neither_panics_nor_spins() {
        let mut zero = revalidation_timer(Duration::ZERO, u64::MAX);
        let started = std::time::Instant::now();
        tokio::time::timeout(Duration::from_secs(1), zero.tick())
            .await
            .expect("a zero period never ticked");
        assert!(started.elapsed() >= REVALIDATE_FLOOR);
        let _huge = revalidation_timer(Duration::MAX, u64::MAX);
    }

    #[test]
    fn the_recheck_jitter_stays_inside_half_a_period_and_differs_between_connections() {
        let every = Duration::from_secs(300);
        for seed in [
            0,
            1,
            7,
            149_999_999_999,
            150_000_000_000,
            u64::MAX / 2,
            u64::MAX,
        ] {
            assert!(jitter(every, seed) < every / 2, "seed {seed}");
        }
        assert_eq!(jitter(Duration::ZERO, u64::MAX), Duration::ZERO);
        assert_eq!(jitter(Duration::from_nanos(1), 5), Duration::ZERO);

        // Connections for one host registering back to back — a reconnect
        // storm — still draw different offsets.
        let offsets: std::collections::BTreeSet<Duration> = (0..16)
            .map(|_| jitter(every, jitter_seed(&host("atlas"))))
            .collect();
        assert!(offsets.len() > 1, "every connection drew the same jitter");
    }

    #[tokio::test]
    async fn the_first_recheck_waits_a_full_period_plus_at_most_half_of_one() {
        let every = Duration::from_millis(100);
        let started = std::time::Instant::now();
        let mut timer = revalidation_timer(every, u64::MAX);
        timer.tick().await;
        let waited = started.elapsed();
        assert!(waited >= every, "{waited:?}");
        assert!(waited < every * 3, "{waited:?}");
    }

    /// A daemon socket whose kernel receive buffer is as small as the OS lets
    /// it be, so a client that stops reading stalls the router's writes after a
    /// handful of frames rather than after megabytes.
    async fn open_with_a_small_receive_buffer(addr: std::net::SocketAddr) -> Ws {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        let stream = socket.connect(addr).await.unwrap();
        tokio_tungstenite::client_async(
            format!("ws://{addr}/v1/connect"),
            tokio_tungstenite::MaybeTlsStream::Plain(stream),
        )
        .await
        .unwrap()
        .0
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_member_that_stops_reading_is_dropped_counted_and_removed_while_others_keep_receiving()
     {
        let mut app = org_app(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_bay", EXAMPLE_REPO),
            ("ghp_cedar", EXAMPLE_REPO),
        ]);
        app.daemon_write_timeout = Duration::from_millis(500);
        // Unmetered: this posts the largest body until a write stalls, far
        // past the post rate and the token budget, neither of which it tests.
        app.rooms = Rooms::unmetered();
        let app = Arc::new(app);
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);

        let mut atlas = daemon(addr, "atlas").await;
        let mut cedar = daemon(addr, "cedar").await;
        let mut bay = open_with_a_small_receive_buffer(addr).await;
        send_frame(
            &mut bay,
            &ClientFrame::Hello {
                role: Role::Daemon,
                token: SecretToken::new("ghp_bay".to_owned()),
            },
        )
        .await;
        assert!(matches!(
            next_frame(&mut bay).await,
            Some(ServerFrame::Welcome { .. })
        ));
        for member in [&mut atlas, &mut bay, &mut cedar] {
            assert!(matches!(
                join(member, &example_repo).await,
                Some(ServerFrame::Joined { .. })
            ));
        }

        // cedar keeps reading everything, on her own task.
        let (cedar_bodies, mut cedar_received) = tokio::sync::mpsc::unbounded_channel::<String>();
        let cedar_reader = tokio::spawn(async move {
            while let Some(frame) = read_frame(&mut cedar).await {
                if let ServerFrame::Message { body, .. } = frame
                    && cedar_bodies.send(body.as_str().to_owned()).is_err()
                {
                    break;
                }
            }
        });

        // bay never reads again. atlas posts the largest message a body can
        // encode to, reading each of her own copies, until the router gives
        // up writing to bay.
        let largest = "\u{1}".repeat(sealmesh_chat_wire::MAX_BODY_CHARS);
        let mut posts = 0u32;
        while app.stalled_writes.load(Ordering::SeqCst) == 0 {
            assert!(
                posts < 2_000,
                "the router kept writing to a member that stopped reading"
            );
            send_frame(&mut atlas, &post(&example_repo, &largest)).await;
            assert!(matches!(
                next_frame(&mut atlas).await,
                Some(ServerFrame::Message { .. })
            ));
            posts += 1;
        }

        tokio::time::timeout(Duration::from_secs(5), async {
            while app.rooms.member_count(&example_repo) != 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the stalled member was never taken out of its room");
        assert_eq!(app.rooms.connection_count(), 2);
        assert_eq!(
            app.stalled_writes.load(Ordering::SeqCst),
            1,
            "only the member that stopped reading stalled"
        );

        send_frame(&mut atlas, &post(&example_repo, "after bay")).await;
        assert_eq!(message_body(next_frame(&mut atlas).await), "after bay");
        let cedar_got_it = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(body) = cedar_received.recv().await {
                if body == "after bay" {
                    return true;
                }
            }
            false
        })
        .await
        .expect("cedar stopped receiving once bay was dropped");
        assert!(cedar_got_it);
        cedar_reader.abort();
    }

    #[test]
    fn the_write_timeout_is_bounded_inside_the_load_balancers_idle_timeout() {
        assert!(WRITE_TIMEOUT >= Duration::from_secs(1), "{WRITE_TIMEOUT:?}");
        assert!(WRITE_TIMEOUT < Duration::from_secs(60), "{WRITE_TIMEOUT:?}");
    }

    #[tokio::test]
    async fn over_the_socket_a_reconnect_does_not_reset_the_post_rate() {
        // The review finding on PR #97: with the rate kept on the connection,
        // reconnect-join-post cycles bought a fresh burst each time.
        let app = Arc::new(org_app(&[("ghp_atlas", EXAMPLE_REPO)]));
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let burst = crate::room_budget::POST_BURST;

        let mut atlas = daemon(addr, "atlas").await;
        assert_eq!(
            join(&mut atlas, &example_repo).await,
            Some(ServerFrame::Joined {
                room: example_repo.clone()
            })
        );
        for i in 0..burst {
            send_frame(&mut atlas, &post(&example_repo, &format!("burst {i}"))).await;
        }
        for _ in 0..burst {
            assert!(matches!(
                next_frame(&mut atlas).await,
                Some(ServerFrame::Message { .. })
            ));
        }
        drop(atlas);
        tokio::time::timeout(Duration::from_secs(5), async {
            while app.rooms.connection_count() != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the first connection was never taken out of the registry");

        let mut atlas = daemon(addr, "atlas").await;
        assert_eq!(
            join(&mut atlas, &example_repo).await,
            Some(ServerFrame::Joined {
                room: example_repo.clone()
            })
        );
        send_frame(&mut atlas, &post(&example_repo, "after reconnect")).await;
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::PostRefused {
                room: example_repo,
                category: PostRefusal::RateLimited
            }),
            "a reconnect handed the host a fresh post burst"
        );
    }

    fn widest_post(room: &RoomId) -> ClientFrame {
        ClientFrame::Post {
            room: room.clone(),
            kind: MessageKind::Claim,
            body: Body::new("😀".repeat(sealmesh_chat_wire::MAX_BODY_CHARS)).unwrap(),
        }
    }

    #[tokio::test]
    async fn over_the_socket_a_post_over_the_rate_limit_is_answered_and_the_room_keeps_flowing() {
        let app = Arc::new(org_app(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_bay", EXAMPLE_REPO),
        ]));
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        let mut bay = daemon(addr, "bay").await;
        join(&mut atlas, &example_repo).await;
        join(&mut bay, &example_repo).await;

        let sent = crate::room_budget::POST_BURST + 4;
        for i in 0..sent {
            send_frame(&mut atlas, &post(&example_repo, &format!("flood {i}"))).await;
        }
        // Every post atlas sends is answered on her own socket exactly once:
        // her own copy of the message, or a refusal.
        let mut seqs = Vec::new();
        let mut refusals = 0u64;
        for _ in 0..sent {
            match next_frame(&mut atlas).await {
                Some(ServerFrame::Message { seq, .. }) => seqs.push(seq.0),
                Some(ServerFrame::PostRefused { room, category }) => {
                    assert_eq!(room, example_repo);
                    assert_eq!(category, PostRefusal::RateLimited);
                    refusals += 1;
                }
                other => panic!("expected a message or a refusal, got {other:?}"),
            }
        }
        assert!(refusals >= 1, "no post over the rate limit was answered");
        let admitted = u64::try_from(seqs.len()).unwrap();
        assert_eq!(
            seqs,
            (1..=admitted).collect::<Vec<_>>(),
            "a refused post used a sequence number"
        );

        send_frame(&mut bay, &post(&example_repo, "from bay")).await;
        match next_frame(&mut atlas).await {
            Some(ServerFrame::Message {
                seq, from, body, ..
            }) => {
                assert_eq!(body.as_str(), "from bay");
                assert_eq!(from, host("bay"));
                assert_eq!(seq, RoomSeq(admitted + 1));
            }
            other => panic!("bay's post did not reach atlas after her flood: {other:?}"),
        }
    }

    #[tokio::test]
    async fn over_the_socket_a_spent_allowance_refuses_that_host_in_that_room_and_nowhere_else() {
        use crate::room_budget::{MAX_POST_TOKENS, TokensPerHour};

        let app = Arc::new(org_app(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_atlas", SKILLS),
            ("ghp_bay", EXAMPLE_REPO),
        ]));
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let skills = repo_room("example-tools", SKILLS);
        let mut atlas = daemon(addr, "atlas").await;
        let mut bay = daemon(addr, "bay").await;
        join(&mut atlas, &example_repo).await;
        join(&mut atlas, &skills).await;
        join(&mut bay, &example_repo).await;

        let fits = TokensPerHour::DEFAULT.get() / MAX_POST_TOKENS;
        for _ in 0..=fits {
            send_frame(&mut atlas, &widest_post(&example_repo)).await;
        }
        let mut delivered = 0u64;
        let mut refused = Vec::new();
        for _ in 0..=fits {
            match next_frame(&mut atlas).await {
                Some(ServerFrame::Message { .. }) => delivered += 1,
                Some(ServerFrame::PostRefused { room, category }) => refused.push((room, category)),
                other => panic!("expected a message or a refusal, got {other:?}"),
            }
        }
        assert_eq!(delivered, fits);
        assert_eq!(
            refused,
            vec![(example_repo.clone(), PostRefusal::BudgetExhausted)],
            "the post past the allowance was not answered as a budget refusal"
        );

        for _ in 0..fits {
            assert!(matches!(
                next_frame(&mut bay).await,
                Some(ServerFrame::Message { .. })
            ));
        }
        send_frame(&mut bay, &widest_post(&example_repo)).await;
        match next_frame(&mut bay).await {
            Some(ServerFrame::Message { from, seq, .. }) => {
                assert_eq!(from, host("bay"));
                assert_eq!(seq, RoomSeq(fits + 1));
            }
            other => panic!("bay was refused for atlas's spending: {other:?}"),
        }
        assert!(matches!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::Message { from, .. }) if from == host("bay")
        ));

        send_frame(&mut atlas, &widest_post(&skills)).await;
        match next_frame(&mut atlas).await {
            Some(ServerFrame::Message {
                room, from, seq, ..
            }) => {
                assert_eq!(room, skills);
                assert_eq!(from, host("atlas"));
                assert_eq!(seq, RoomSeq(1));
            }
            other => panic!("atlas was refused in a room she had not spent in: {other:?}"),
        }
    }

    #[tokio::test]
    async fn over_the_socket_two_daemons_left_posting_back_and_forth_stop_on_the_turn_cap() {
        // The test XYZ-64 is for. Two daemons, no third party, each answering
        // the other's post as soon as it arrives, with the router's real
        // limits: the default post rate, token allowance and turn cap. A
        // rate-limited post is retried after one refill, the way an agent
        // that reads `rate_limited` as "slow down" would. The rate therefore
        // never ends the exchange, and neither does the allowance, which
        // these short posts barely touch. Only the turn cap can.
        use crate::turn_cap::TurnCap;

        let app = Arc::new(org_app(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_bay", EXAMPLE_REPO),
            ("ghp_cedar", EXAMPLE_REPO),
        ]));
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        let mut bay = daemon(addr, "bay").await;
        join(&mut atlas, &example_repo).await;
        join(&mut bay, &example_repo).await;

        let cap = u64::from(TurnCap::DEFAULT.get());
        // A bound on the test, well past the cap, so a router without one
        // fails here instead of looping forever.
        let give_up = cap + 10;
        let mut delivered = 0u64;
        let mut speaker = 0usize;
        let mut stopped = None;
        while delivered < give_up && stopped.is_none() {
            let (me, other) = if speaker == 0 {
                (&mut atlas, &mut bay)
            } else {
                (&mut bay, &mut atlas)
            };
            send_frame(me, &post(&example_repo, &format!("turn {delivered}"))).await;
            match next_frame(me).await {
                Some(ServerFrame::Message { seq, .. }) => {
                    assert_eq!(seq, RoomSeq(delivered + 1));
                    assert!(matches!(
                        next_frame(other).await,
                        Some(ServerFrame::Message { .. })
                    ));
                    delivered += 1;
                    speaker = 1 - speaker;
                }
                Some(ServerFrame::PostRefused {
                    category: PostRefusal::RateLimited,
                    ..
                }) => tokio::time::sleep(crate::room_budget::POST_REFILL).await,
                Some(ServerFrame::PostRefused { room, category }) => {
                    assert_eq!(room, example_repo);
                    stopped = Some((speaker, category));
                }
                other => panic!("expected a message or a refusal, got {other:?}"),
            }
        }
        assert_eq!(
            stopped,
            Some((speaker, PostRefusal::TurnCapped)),
            "two daemons exchanged {delivered} posts and the turn cap never stopped them"
        );
        assert_eq!(
            delivered, cap,
            "the exchange did not stop exactly at the cap"
        );

        // Both agents see the stop: the poster was refused above, and the
        // other side is told without having posted anything.
        let (capped, told) = if speaker == 0 {
            (&mut atlas, &mut bay)
        } else {
            (&mut bay, &mut atlas)
        };
        assert_eq!(
            next_frame(told).await,
            Some(ServerFrame::TurnCapped {
                room: example_repo.clone()
            }),
            "the participant who did not post was never told the exchange was capped"
        );

        // Both sides stay refused, and neither is told again however often
        // the other retries.
        let refused = Some(ServerFrame::PostRefused {
            room: example_repo.clone(),
            category: PostRefusal::TurnCapped,
        });
        for _ in 0..3 {
            send_frame(told, &post(&example_repo, "one more")).await;
            assert_eq!(next_frame(told).await, refused);
            send_frame(capped, &post(&example_repo, "one more")).await;
            assert_eq!(next_frame(capped).await, refused);
        }
        assert!(quiet_for(told, Duration::from_millis(100)).await);
        assert!(quiet_for(capped, Duration::from_millis(100)).await);

        // Nothing else a daemon can send resets it: not a second `Hello`
        // naming someone else, not a re-join of the room it holds, not a
        // stray answer or ask, and not a projects request, which a daemon's
        // connection answers with the map and nothing more (rule 12).
        for frame in [
            ClientFrame::Hello {
                role: Role::Daemon,
                token: SecretToken::new("ghp_cedar".to_owned()),
            },
            ClientFrame::Projects {},
            ClientFrame::Ask { id: QueryId(1) },
            ClientFrame::Answer {
                id: QueryId(2),
                state: sealmesh_proto::HostState::NotSharing {},
            },
            ClientFrame::Join {
                room: example_repo.clone(),
            },
        ] {
            send_frame(capped, &frame).await;
        }
        // The map is cut on its own branch, so it may come either side of
        // the join's answer.
        let mut answers = vec![next_frame(capped).await, next_frame(capped).await];
        answers.sort_by_key(|frame| matches!(frame, Some(ServerFrame::Projects { .. })));
        assert_eq!(
            answers,
            vec![
                Some(ServerFrame::Joined {
                    room: example_repo.clone()
                }),
                Some(ServerFrame::Projects {
                    projects: vec![sealmesh_proto::ProjectEntry {
                        id: sealmesh_wire::ProjectId::try_from("example-repo").unwrap(),
                        repos: vec![RepoSlug::try_from(EXAMPLE_REPO).unwrap()],
                    }],
                }),
            ]
        );
        send_frame(capped, &post(&example_repo, "and now?")).await;
        assert_eq!(next_frame(capped).await, refused);

        // Leaving and rejoining, until the room re-forms, resets nothing.
        for ws in [&mut atlas, &mut bay] {
            send_frame(
                ws,
                &ClientFrame::Leave {
                    room: example_repo.clone(),
                },
            )
            .await;
            assert_eq!(
                next_frame(ws).await,
                Some(ServerFrame::Left {
                    room: example_repo.clone()
                })
            );
        }
        assert_eq!(app.rooms.room_count(), 0, "the room did not empty");
        join(&mut atlas, &example_repo).await;
        join(&mut bay, &example_repo).await;
        for ws in [&mut atlas, &mut bay] {
            send_frame(ws, &post(&example_repo, "fresh start?")).await;
            assert_eq!(
                next_frame(ws).await,
                refused,
                "a re-formed room reset the cap"
            );
        }

        // A third host posting does.
        let mut cedar = daemon(addr, "cedar").await;
        join(&mut cedar, &example_repo).await;
        send_frame(&mut cedar, &post(&example_repo, "cedar here")).await;
        for ws in [&mut cedar, &mut atlas, &mut bay] {
            assert_eq!(message_body(next_frame(ws).await), "cedar here");
        }
        // atlas spent her last post-rate token just before the cap, and every
        // refused post since spent nothing, so she waits one refill like any
        // rate-limited poster.
        tokio::time::sleep(crate::room_budget::POST_REFILL).await;
        send_frame(&mut atlas, &post(&example_repo, "thanks cedar")).await;
        match next_frame(&mut atlas).await {
            Some(ServerFrame::Message { from, seq, .. }) => {
                assert_eq!(from, host("atlas"));
                // The room re-formed above, and its numbering did **not**
                // restart: XYZ-61 seeds a re-formed room from the sequence
                // its history already holds. The exchange persisted `cap`
                // messages, so cedar's post took `cap + 1` and this one takes
                // `cap + 2`. None of the refused posts in between took a
                // number. Before seeding this read `RoomSeq(2)`, which is the
                // reuse the conditional write now refuses.
                assert_eq!(seq, RoomSeq(cap + 2));
            }
            other => panic!("a third host's post did not lift the cap: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_post_in_flight_does_not_stop_its_connection_answering_a_query() {
        // Blocking 1 from the XYZ-61 review. The post used to be awaited
        // inline in a `select!` arm body, and an arm body runs to completion
        // before the loop iterates — so for as long as the write took, this
        // connection could not write a queued `ServerFrame::Query` to its
        // socket or read the `Answer` back. A slow table therefore made every
        // posting daemon read as `Unreachable` to `who`, turning a chat
        // outage into a `who` outage. `who` is the product, so this is the
        // invariant `rooms.rs` states and the join path already honours by
        // pinning its check into a select *branch*.
        //
        // Two things make this deterministic rather than a race, and both
        // matter: an earlier version of this test passed against the parked
        // code and proved nothing.
        //
        // The query is not asked until the store reports the write has
        // actually begun, so the connection is provably inside the post
        // rather than racing `select!`, which picks freely among ready
        // branches. And the window allowed for the `Query` to arrive is far
        // shorter than the write instead of being `FANOUT_TIMEOUT` — that is
        // 5s, as long as the `PERSIST_BUDGET` ceiling on any write, so a
        // parked connection would still have answered inside it.
        const WRITE: Duration = Duration::from_secs(2);
        const WHILE_WRITING: Duration = Duration::from_millis(300);

        let mut app = org_app(&[("ghp_atlas", EXAMPLE_REPO)]);
        let slow = Arc::new(Fake::slow(WRITE));
        // The turbofish pins the clone to `Arc<Fake>` so the annotation can
        // unsize it; leaving it off makes inference demand an
        // `&Arc<dyn RoomHistory>` argument, which `slow` is not — and `slow`
        // has to stay an `Arc<Fake>` here so the test can read `appends()`.
        let history: Arc<dyn RoomHistory> = Arc::<Fake>::clone(&slow);
        app.rooms = Rooms::with_limits(
            crate::room_budget::TokensPerHour::DEFAULT,
            crate::turn_cap::TurnCap::DEFAULT,
            history,
        );
        let app = Arc::new(app);
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        join(&mut atlas, &example_repo).await;

        send_frame(&mut atlas, &post(&example_repo, "while this is in flight")).await;

        // `appends` is counted before the store sleeps, so this waits until
        // the write is genuinely under way.
        let step = Duration::from_millis(5);
        let mut waited = Duration::ZERO;
        while slow.appends() == 0 {
            assert!(waited < WRITE, "the router never started the write");
            tokio::time::sleep(step).await;
            waited = waited.saturating_add(step);
        }

        let reader = host("reader");
        let answering = async {
            // The `Query` has to reach this socket while the write is still
            // running, not once it has finished.
            match tokio::time::timeout(WHILE_WRITING, next_frame(&mut atlas)).await {
                Ok(Some(ServerFrame::Query { id, .. })) => {
                    send_frame(
                        &mut atlas,
                        &ClientFrame::Answer {
                            id,
                            state: sealmesh_proto::HostState::NotSharing {},
                        },
                    )
                    .await;
                }
                other => panic!(
                    "this connection was parked by its own post and could not be asked: {other:?}"
                ),
            }
        };
        let (report, ()) = tokio::join!(
            crate::query::ask_all(&app.registry, &reader, FANOUT_TIMEOUT),
            answering
        );
        assert_eq!(
            report,
            vec![(host("atlas"), sealmesh_proto::HostState::NotSharing {})],
            "the daemon read as unreachable while its own post was in flight"
        );

        // And the post still lands once the write finishes.
        assert_eq!(
            message_body(next_frame(&mut atlas).await),
            "while this is in flight"
        );
    }

    const PORTAL: &str = "example-org/example-repo-portal";

    fn ticket_room(number: u32) -> RoomId {
        RoomId::new(
            sealmesh_wire::ProjectId::try_from("example-repo").unwrap(),
            Scope::Ticket(sealmesh_wire::TicketId::new("ENG", number).unwrap()),
        )
    }

    #[tokio::test]
    async fn a_merge_check_in_flight_does_not_stop_its_connection_answering_a_query() {
        // XYZ-62. The merge check waits on GitHub, and the invariant is the
        // one the post path already holds: that wait is inside the pinned
        // post future, polled as its own `select!` branch, never awaited in an
        // arm body. Deterministic the same two ways as the test above: the
        // query is asked only once the directory reports the check has
        // started, and the `Query` must arrive in a window far shorter than
        // the check — which here never finishes until the test lets it.
        const WHILE_CHECKING: Duration = Duration::from_millis(300);

        let app = org_app(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_atlas", PORTAL)]);
        app.directory
            .pulls_hang
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let app = Arc::new(app);
        let addr = serve(Arc::clone(&app)).await;
        let ticket = ticket_room(12);
        let mut atlas = daemon(addr, "atlas").await;
        assert_eq!(
            join(&mut atlas, &ticket).await,
            Some(ServerFrame::Joined {
                room: ticket.clone()
            })
        );

        send_frame(&mut atlas, &post(&ticket, "while the check runs")).await;
        let step = Duration::from_millis(5);
        let mut waited = Duration::ZERO;
        while app
            .directory
            .pulls_started
            .load(std::sync::atomic::Ordering::SeqCst)
            == 0
        {
            assert!(
                waited < Duration::from_secs(5),
                "the router never started the check"
            );
            tokio::time::sleep(step).await;
            waited = waited.saturating_add(step);
        }

        let reader = host("reader");
        let answering = async {
            match tokio::time::timeout(WHILE_CHECKING, next_frame(&mut atlas)).await {
                Ok(Some(ServerFrame::Query { id, .. })) => {
                    send_frame(
                        &mut atlas,
                        &ClientFrame::Answer {
                            id,
                            state: sealmesh_proto::HostState::NotSharing {},
                        },
                    )
                    .await;
                }
                other => panic!(
                    "this connection was parked by its own merge check and could not be \
                     asked: {other:?}"
                ),
            }
        };
        let (report, ()) = tokio::join!(
            crate::query::ask_all(&app.registry, &reader, FANOUT_TIMEOUT),
            answering
        );
        assert_eq!(
            report,
            vec![(host("atlas"), sealmesh_proto::HostState::NotSharing {})],
            "the daemon read as unreachable while its own merge check was in flight"
        );

        // Still in flight when the query was answered, so the answer did not
        // come from a check that had already finished.
        assert!(
            app.directory
                .pulls_hang
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        app.directory
            .pulls_hang
            .store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            message_body(next_frame(&mut atlas).await),
            "while the check runs"
        );
    }

    #[tokio::test]
    async fn over_the_socket_a_merged_ticket_room_answers_archived_and_stays_joinable() {
        let app = org_app(&[
            ("ghp_atlas", EXAMPLE_REPO),
            ("ghp_atlas", PORTAL),
            ("ghp_bay", EXAMPLE_REPO),
            ("ghp_bay", PORTAL),
        ]);
        *app.directory.pulls.lock().unwrap() = Ok(vec![crate::auth::PullSummary {
            head_ref: "feature/eng-12-portal".to_owned(),
            into_default_branch: true,
            state: crate::auth::PullState::Merged,
        }]);
        let app = Arc::new(app);
        let addr = serve(Arc::clone(&app)).await;
        let ticket = ticket_room(12);
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        join(&mut atlas, &ticket).await;
        join(&mut atlas, &example_repo).await;

        let archived = Some(ServerFrame::PostRefused {
            room: ticket.clone(),
            category: PostRefusal::Archived,
        });
        send_frame(&mut atlas, &post(&ticket, "after the merge")).await;
        assert_eq!(next_frame(&mut atlas).await, archived);
        // Refused, not dropped, every time.
        send_frame(&mut atlas, &post(&ticket, "again")).await;
        assert_eq!(next_frame(&mut atlas).await, archived);

        // The repo room in the same project is untouched.
        send_frame(&mut atlas, &post(&example_repo, "repo room")).await;
        assert_eq!(message_body(next_frame(&mut atlas).await), "repo room");

        // And the archived room can still be joined.
        let mut bay = daemon(addr, "bay").await;
        assert_eq!(
            join(&mut bay, &ticket).await,
            Some(ServerFrame::Joined {
                room: ticket.clone()
            })
        );
        send_frame(&mut bay, &post(&ticket, "late")).await;
        assert_eq!(next_frame(&mut bay).await, archived);
    }

    #[test]
    fn the_pending_post_queue_is_exactly_one_burst_wide() {
        // If these drift apart, pipelining a legal burst starts being refused.
        assert_eq!(
            u64::try_from(MAX_PENDING_POSTS).unwrap(),
            crate::room_budget::POST_BURST,
            "the queue must admit a full burst"
        );
    }

    /// A router whose history writes take `write`, and the store behind it.
    fn app_over_a_slow_store(write: Duration) -> (Arc<App<Org>>, Arc<Fake>) {
        let mut app = org_app(&[("ghp_atlas", EXAMPLE_REPO)]);
        let slow = Arc::new(Fake::slow(write));
        let history: Arc<dyn RoomHistory> = Arc::<Fake>::clone(&slow);
        app.rooms = Rooms::with_limits(
            crate::room_budget::TokensPerHour::DEFAULT,
            crate::turn_cap::TurnCap::DEFAULT,
            history,
        );
        (Arc::new(app), slow)
    }

    #[tokio::test]
    async fn a_burst_pipelined_without_reading_the_answers_is_still_delivered_in_order() {
        // The burst the token budget promises, sent the way a daemon is
        // allowed to send it: all of it at once, reading nothing back until
        // the end. Refusing the posts behind the one being recorded — the
        // first shape of this fix — withdrew that allowance, and this is the
        // contract two existing socket tests already depend on.
        let (app, slow) = app_over_a_slow_store(Duration::from_millis(40));
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        join(&mut atlas, &example_repo).await;

        let burst = usize::try_from(crate::room_budget::POST_BURST).unwrap();
        for i in 0..burst {
            send_frame(&mut atlas, &post(&example_repo, &format!("burst {i}"))).await;
        }
        for i in 0..burst {
            assert_eq!(
                message_body(next_frame(&mut atlas).await),
                format!("burst {i}"),
                "post {i} of a legal burst was refused or delivered out of order"
            );
        }
        assert_eq!(slow.appends(), u64::try_from(burst).unwrap());
        assert_eq!(slow.seqs_of(&example_repo).len(), burst);
    }

    #[tokio::test]
    async fn a_post_past_the_queue_is_refused_at_once_rather_than_held() {
        // The bound on what one daemon can make the router hold. Past a burst
        // the answer arrives while the first write is still running, rather
        // than being queued behind it.
        const WRITE: Duration = Duration::from_secs(2);
        const AT_ONCE: Duration = Duration::from_millis(300);

        let (app, slow) = app_over_a_slow_store(WRITE);
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        join(&mut atlas, &example_repo).await;

        send_frame(&mut atlas, &post(&example_repo, "first")).await;
        let step = Duration::from_millis(5);
        let mut waited = Duration::ZERO;
        while slow.appends() == 0 {
            assert!(waited < WRITE, "the router never started the write");
            tokio::time::sleep(step).await;
            waited = waited.saturating_add(step);
        }

        // One in flight plus a full queue, then one more.
        for i in 0..=MAX_PENDING_POSTS {
            send_frame(&mut atlas, &post(&example_repo, &format!("queued {i}"))).await;
        }
        match tokio::time::timeout(AT_ONCE, next_frame(&mut atlas)).await {
            Ok(Some(ServerFrame::PostRefused { room, category })) => {
                assert_eq!(room, example_repo);
                assert_eq!(category, PostRefusal::RateLimited);
            }
            other => panic!("a post past the queue was not refused at once: {other:?}"),
        }
        assert_eq!(
            slow.appends(),
            1,
            "a queued post reached the store while the first was still writing"
        );
    }

    #[tokio::test]
    async fn a_leave_discards_this_connections_posts_the_room_has_not_recorded_yet() {
        // The property `over_the_socket_a_room_that_re_forms_never_resends_…`
        // caught when the posts behind the one being recorded were first put
        // in a queue: `old2` was still queued when the `Leave` was read, so it
        // was recorded after the rejoin and delivered into the room's next
        // life. A slow store makes that one round deterministic, where the
        // other test needed 45 to hit it.
        let (app, slow) = app_over_a_slow_store(Duration::from_millis(400));
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        join(&mut atlas, &example_repo).await;

        send_frame(&mut atlas, &post(&example_repo, "old1")).await;
        send_frame(&mut atlas, &post(&example_repo, "old2")).await;
        send_frame(
            &mut atlas,
            &ClientFrame::Leave {
                room: example_repo.clone(),
            },
        )
        .await;
        send_frame(
            &mut atlas,
            &ClientFrame::Join {
                room: example_repo.clone(),
            },
        )
        .await;

        // Read through to the rejoin. A post that had already started before
        // the leave may still arrive, but only before the `Left`.
        let mut left = false;
        loop {
            match next_frame(&mut atlas).await {
                Some(ServerFrame::Message { body, .. }) => {
                    assert!(!left, "{} arrived after the Left", body.as_str());
                }
                Some(ServerFrame::Left { .. }) => left = true,
                Some(ServerFrame::Joined { .. }) => break,
                other => panic!("unexpected {other:?}"),
            }
        }

        send_frame(&mut atlas, &post(&example_repo, "sentinel")).await;
        assert_eq!(
            message_body(next_frame(&mut atlas).await),
            "sentinel",
            "a post from before the Leave was recorded after the rejoin"
        );
        assert!(
            !slow.bodies_of(&example_repo).contains(&"old2".to_owned()),
            "a post the Leave should have discarded was written: {:?}",
            slow.bodies_of(&example_repo)
        );
    }

    #[tokio::test]
    async fn a_post_queued_for_a_room_left_is_not_recorded_after_the_rejoin() {
        // The queued half of the cancellation, which needs an in-flight post
        // to a *different* room to be reachable at all. That post is not
        // cancelled by this `Leave`, and the queued one cannot start behind
        // it, so the rejoin admits first — and without dropping the queue,
        // the queued post would then reserve against the room's next life.
        //
        // `a_leave_discards_this_connections_posts_the_room_has_not_recorded_yet`
        // does not reach this: there the in-flight post is cancelled too, so
        // the queue drains against a membership that is simply gone.
        const WRITE: Duration = Duration::from_millis(700);

        let mut app = org_app(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_atlas", SKILLS)]);
        let slow = Arc::new(Fake::slow(WRITE));
        let history: Arc<dyn RoomHistory> = Arc::<Fake>::clone(&slow);
        app.rooms = Rooms::with_limits(
            crate::room_budget::TokensPerHour::DEFAULT,
            crate::turn_cap::TurnCap::DEFAULT,
            history,
        );
        let app = Arc::new(app);
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let skills = repo_room("example-tools", SKILLS);
        let mut atlas = daemon(addr, "atlas").await;
        join(&mut atlas, &example_repo).await;
        join(&mut atlas, &skills).await;

        // In flight, and to another room, so this `Leave` leaves it alone.
        send_frame(&mut atlas, &post(&skills, "slow elsewhere")).await;
        let step = Duration::from_millis(5);
        let mut waited = Duration::ZERO;
        while slow.appends() == 0 {
            assert!(waited < WRITE, "the router never started the write");
            tokio::time::sleep(step).await;
            waited = waited.saturating_add(step);
        }

        send_frame(&mut atlas, &post(&example_repo, "doomed")).await;
        send_frame(
            &mut atlas,
            &ClientFrame::Leave {
                room: example_repo.clone(),
            },
        )
        .await;
        send_frame(
            &mut atlas,
            &ClientFrame::Join {
                room: example_repo.clone(),
            },
        )
        .await;
        send_frame(&mut atlas, &post(&example_repo, "sentinel")).await;

        // Asserted against the store, not the socket: two rooms' frames
        // interleave here, and what is at stake is what got written. The
        // sentinel is queued behind "doomed", so once it appears, anything
        // wrongly recorded before it has appeared too.
        let deadline = Duration::from_secs(10);
        let mut waited = Duration::ZERO;
        while !slow
            .bodies_of(&example_repo)
            .contains(&"sentinel".to_owned())
        {
            assert!(waited < deadline, "the sentinel was never recorded");
            tokio::time::sleep(step).await;
            waited = waited.saturating_add(step);
        }
        assert_eq!(
            slow.bodies_of(&example_repo),
            vec!["sentinel".to_owned()],
            "a post queued before the Leave was recorded after the rejoin"
        );
    }

    #[tokio::test]
    async fn a_post_parked_when_a_recheck_revokes_is_not_recorded_after_the_rejoin() {
        // Finding 1 of the re-review: a re-check revocation ends a membership
        // with no client frame at all, so the `Leave` arm never sees it and
        // only the `in_flight` branch can cancel.
        //
        // The post has to be parked *before* `reserve` for this to bite —
        // `reserve` runs ahead of the append, so a post already past it has
        // legitimately taken its number. Parking it is what the second daemon
        // is for: bay's slow write holds example-repo's ingest lock, so atlas's
        // post to the same room cannot reserve until it is released, and the
        // revocation and the rejoin both land in the meantime. The same room,
        // because the lock is per room: a write elsewhere parks nothing.
        const WRITE: Duration = Duration::from_millis(700);

        let mut app = org_app(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)]);
        let slow = Arc::new(Fake::slow(WRITE));
        let history: Arc<dyn RoomHistory> = Arc::<Fake>::clone(&slow);
        app.rooms = Rooms::with_limits(
            crate::room_budget::TokensPerHour::DEFAULT,
            crate::turn_cap::TurnCap::DEFAULT,
            history,
        );
        app.room_revalidate_every = Duration::from_millis(100);
        let app = Arc::new(app);
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);

        let mut bay = daemon(addr, "bay").await;
        join(&mut bay, &example_repo).await;
        let mut atlas = daemon(addr, "atlas").await;
        join(&mut atlas, &example_repo).await;

        // bay holds example-repo's ingest lock for most of a second.
        send_frame(&mut bay, &post(&example_repo, "holding the lock")).await;
        let step = Duration::from_millis(5);
        let mut waited = Duration::ZERO;
        while slow.appends() == 0 {
            assert!(waited < WRITE, "the router never started bay's write");
            tokio::time::sleep(step).await;
            waited = waited.saturating_add(step);
        }

        // atlas's post is now parked on that lock, short of `reserve`.
        send_frame(&mut atlas, &post(&example_repo, "doomed")).await;
        app.directory.revoke("ghp_atlas", EXAMPLE_REPO);

        // The re-check removes her, and no `Leave` frame was ever sent. bay
        // is in the room too, so the removal need not be the next frame she
        // is sent; nothing of bay's reaches her before his write lands.
        assert!(
            slow.bodies_of(&example_repo).is_empty(),
            "bay's write finished before the revocation, so nothing was parked"
        );
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::Left {
                room: example_repo.clone()
            }),
            "the recheck did not remove atlas"
        );
        assert!(
            slow.bodies_of(&example_repo).is_empty(),
            "bay's write finished before the removal, so nothing was parked"
        );

        // She is let back in and rejoins, still inside bay's write.
        app.directory
            .grants
            .lock()
            .unwrap()
            .push(("ghp_atlas".to_owned(), EXAMPLE_REPO.to_owned()));
        join(&mut atlas, &example_repo).await;
        send_frame(&mut atlas, &post(&example_repo, "sentinel")).await;

        // Asserted against the store: "doomed" would be written before the
        // sentinel, so once the sentinel is there, a misfile would be too.
        let deadline = Duration::from_secs(10);
        let mut waited = Duration::ZERO;
        while !slow
            .bodies_of(&example_repo)
            .contains(&"sentinel".to_owned())
        {
            assert!(waited < deadline, "the sentinel was never recorded");
            tokio::time::sleep(step).await;
            waited = waited.saturating_add(step);
        }
        assert_eq!(
            slow.bodies_of(&example_repo),
            vec!["holding the lock".to_owned(), "sentinel".to_owned()],
            "a post parked when the recheck revoked was recorded after the rejoin"
        );
    }

    /// `HostState` as a router built before `Throttled` knows it.
    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize)]
    #[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
    enum HostStateBeforeThrottled {
        Answering {
            snapshot: Box<sealmesh_wire::Snapshot>,
        },
        NotSharing,
        Unreachable,
    }

    /// `ClientFrame` as a router built before `Throttled` knows it.
    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize)]
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
            batch: sealmesh_proto::EventBatch,
        },
        Projects {},
    }

    #[test]
    fn a_throttled_answer_is_undecodable_to_an_old_router_and_a_frame_to_this_one() {
        // An old router's `serve_daemon` counts an undecodable frame and keeps
        // the socket; the unanswered ticket then times out to `Unreachable`
        // (`query::a_silent_daemon_times_out_without_holding_the_others`).
        let throttled = serde_json::to_vec(&ClientFrame::Answer {
            id: QueryId(4),
            state: sealmesh_proto::HostState::Throttled {},
        })
        .unwrap();
        assert!(matches!(
            classify::<ClientFrameBeforeThrottled>(&throttled),
            Received::Undecodable
        ));
        assert!(matches!(
            classify::<ClientFrame>(&throttled),
            Received::Frame(ClientFrame::Answer {
                id: QueryId(4),
                state: sealmesh_proto::HostState::Throttled {}
            })
        ));
    }

    // Backfill and the room list, over a real socket.

    /// A router with `grants` whose room history is `store`.
    fn org_app_over(grants: &[(&str, &str)], store: &Arc<Fake>) -> Arc<App<Org>> {
        let mut app = org_app(grants);
        let history: Arc<dyn RoomHistory> = Arc::<Fake>::clone(store);
        app.rooms = Rooms::new(history);
        Arc::new(app)
    }

    /// Waits until `ready`, failing after five seconds.
    async fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
        let step = Duration::from_millis(5);
        let mut waited = Duration::ZERO;
        while !ready() {
            assert!(waited < Duration::from_secs(5), "{what} never happened");
            tokio::time::sleep(step).await;
            waited = waited.saturating_add(step);
        }
    }

    /// Asks every attached daemon while `daemon` answers, and requires the
    /// `Query` to reach it inside `window`, a span far shorter than whatever
    /// the caller has in flight. A generous window proves nothing: a parked
    /// connection still answers once its read finishes.
    async fn answers_a_query_inside(
        app: &App<Org>,
        daemon: &mut Ws,
        login: &str,
        window: Duration,
    ) {
        let reader = host("reader");
        let answering = async {
            match tokio::time::timeout(window, next_frame(daemon)).await {
                Ok(Some(ServerFrame::Query { id, .. })) => {
                    send_frame(
                        daemon,
                        &ClientFrame::Answer {
                            id,
                            state: sealmesh_proto::HostState::NotSharing {},
                        },
                    )
                    .await;
                }
                other => panic!("this connection was parked and could not be asked: {other:?}"),
            }
        };
        let (report, ()) = tokio::join!(
            crate::query::ask_all(&app.registry, &reader, FANOUT_TIMEOUT),
            answering
        );
        assert_eq!(
            report,
            vec![(host(login), sealmesh_proto::HostState::NotSharing {})],
            "the daemon read as unreachable while its own read was in flight"
        );
    }

    fn stored(room: &RoomId, seq: u64, from: &str, text: &str) -> crate::history::Record {
        crate::history::Record {
            room: room.clone(),
            seq: RoomSeq(seq),
            from: host(from),
            kind: MessageKind::Claim,
            body: Body::new(text).unwrap(),
            // A minute ago, not a fixed epoch second: XYZ-108 makes the read
            // path filter on `expires_at`, computed from `posted_at`, against
            // the real clock. A fixed historical timestamp eventually — and
            // for a worktree checked out well after this was written,
            // immediately — reads as already expired and vanishes from every
            // page (worker contract rule 4: a fixture must not depend on when
            // it happens to run).
            posted_at: crate::history::now_epoch()
                .unwrap_or(1_700_000_000)
                .saturating_sub(60),
        }
    }

    fn entry(seq: u64, from: &str, text: &str) -> sealmesh_proto::HistoryEntry {
        sealmesh_proto::HistoryEntry {
            seq: RoomSeq(seq),
            from: host(from),
            kind: MessageKind::Claim,
            body: Body::new(text).unwrap(),
        }
    }

    fn backfill(room: &RoomId, after: Option<u64>) -> ClientFrame {
        ClientFrame::Backfill {
            room: room.clone(),
            after: after.map(RoomSeq),
        }
    }

    fn not_held(room: &RoomId) -> ServerFrame {
        ServerFrame::BackfillRefused {
            room: room.clone(),
            category: BackfillRefusal::NotHeld,
        }
    }

    #[tokio::test]
    async fn a_backfill_in_flight_does_not_stop_its_connection_answering_a_query() {
        // The hard constraint from XYZ-61's review, for the read path. The
        // query is asked only once the store reports the read has begun, and
        // the `Query` must arrive in a window a small fraction of the read.
        const READ: Duration = Duration::from_secs(2);
        const WHILE_READING: Duration = Duration::from_millis(300);

        let store = Arc::new(Fake::new());
        let app = org_app_over(&[("ghp_atlas", EXAMPLE_REPO)], &store);
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        join(&mut atlas, &example_repo).await;
        store.insert(stored(&example_repo, 1, "bay", "before atlas joined"));
        store.set_page_delay(READ);

        send_frame(&mut atlas, &backfill(&example_repo, None)).await;
        wait_until("the backfill read starting", || store.page_reads() > 0).await;
        let started = std::time::Instant::now();

        answers_a_query_inside(&app, &mut atlas, "atlas", WHILE_READING).await;

        // One read per connection: a second, sent while the first is still
        // being read, is refused at once rather than queued behind it.
        send_frame(&mut atlas, &backfill(&example_repo, None)).await;
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::BackfillRefused {
                room: example_repo.clone(),
                category: BackfillRefusal::Busy,
            })
        );
        assert!(
            started.elapsed() < READ,
            "the busy refusal waited for the read: {:?}",
            started.elapsed()
        );
        assert_eq!(store.page_reads(), 1, "the busy request read the store");

        // And the first read still answers once the store does.
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::Backfill {
                room: example_repo,
                after: None,
                messages: vec![entry(1, "bay", "before atlas joined")],
                next: None,
            })
        );
    }

    #[tokio::test]
    async fn a_room_listing_in_flight_does_not_stop_its_connection_answering_a_query() {
        const WHILE_LISTING: Duration = Duration::from_millis(300);

        let app = Arc::new(org_app(&[("ghp_atlas", EXAMPLE_REPO)]));
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        join(&mut atlas, &example_repo).await;

        // Every access check from here waits until the test lets it go.
        app.directory
            .hang
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let before = app
            .directory
            .reads_started
            .load(std::sync::atomic::Ordering::SeqCst);
        send_frame(&mut atlas, &ClientFrame::ListRooms {}).await;
        wait_until("the listing's access check starting", || {
            app.directory
                .reads_started
                .load(std::sync::atomic::Ordering::SeqCst)
                > before
        })
        .await;

        answers_a_query_inside(&app, &mut atlas, "atlas", WHILE_LISTING).await;

        send_frame(&mut atlas, &ClientFrame::ListRooms {}).await;
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::RoomListRefused {})
        );

        // Still in flight when the query was answered.
        app.directory
            .hang
            .store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::RoomList {
                rooms: sealmesh_proto::RoomList::new(vec![example_repo]).unwrap(),
            })
        );
    }

    fn projects_frame(entries: &[(&str, &[&str])]) -> ServerFrame {
        ServerFrame::Projects {
            projects: entries
                .iter()
                .map(|(id, repos)| sealmesh_proto::ProjectEntry {
                    id: sealmesh_wire::ProjectId::try_from(*id).unwrap(),
                    repos: repos
                        .iter()
                        .map(|r| RepoSlug::try_from(*r).unwrap())
                        .collect(),
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn a_daemon_is_answered_the_project_map_its_own_token_can_read_off_the_check_allowance() {
        let app = Arc::new(org_app(&[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", SKILLS)]));
        // Spend atlas's whole check allowance first: the map must still come
        // back whole, because it is not charged to it.
        while app.check_budgets.try_charge(&host("atlas")) {}
        let addr = serve(Arc::clone(&app)).await;

        let mut atlas = daemon(addr, "atlas").await;
        send_frame(&mut atlas, &ClientFrame::Projects {}).await;
        // Only the repo atlas can read, of the project it is in; nothing of
        // the project she has no repo in.
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(projects_frame(&[("example-repo", &[EXAMPLE_REPO])]))
        );

        let mut bay = daemon(addr, "bay").await;
        send_frame(&mut bay, &ClientFrame::Projects {}).await;
        assert_eq!(
            next_frame(&mut bay).await,
            Some(projects_frame(&[("example-tools", &[SKILLS])]))
        );

        // A daemon that can read nothing mapped is answered an empty map,
        // which is the answer for a map with nothing in it at all.
        let mut cedar = daemon(addr, "cedar").await;
        send_frame(&mut cedar, &ClientFrame::Projects {}).await;
        assert_eq!(next_frame(&mut cedar).await, Some(projects_frame(&[])));
    }

    #[tokio::test]
    async fn a_project_map_in_flight_does_not_stop_its_connection_answering_a_query() {
        const WHILE_MAPPING: Duration = Duration::from_millis(300);

        let app = Arc::new(org_app(&[("ghp_atlas", EXAMPLE_REPO)]));
        let addr = serve(Arc::clone(&app)).await;
        let mut atlas = daemon(addr, "atlas").await;

        app.directory
            .hang
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let before = app
            .directory
            .reads_started
            .load(std::sync::atomic::Ordering::SeqCst);
        send_frame(&mut atlas, &ClientFrame::Projects {}).await;
        wait_until("the map's access check starting", || {
            app.directory
                .reads_started
                .load(std::sync::atomic::Ordering::SeqCst)
                > before
        })
        .await;

        answers_a_query_inside(&app, &mut atlas, "atlas", WHILE_MAPPING).await;

        // A second request while the first runs is not a second cut.
        let started = app
            .directory
            .reads_started
            .load(std::sync::atomic::Ordering::SeqCst);
        send_frame(&mut atlas, &ClientFrame::Projects {}).await;
        assert!(quiet_for(&mut atlas, Duration::from_millis(100)).await);
        assert_eq!(
            app.directory
                .reads_started
                .load(std::sync::atomic::Ordering::SeqCst),
            started,
            "the duplicate started a cut of its own"
        );

        app.directory
            .hang
            .store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(projects_frame(&[("example-repo", &[EXAMPLE_REPO])]))
        );
        assert!(quiet_for(&mut atlas, Duration::from_millis(200)).await);
    }

    #[tokio::test]
    async fn a_repeat_project_map_request_within_the_cooldown_is_answered_without_a_cut() {
        let app = Arc::new(org_app(&[("ghp_atlas", EXAMPLE_REPO)]));
        let addr = serve(Arc::clone(&app)).await;
        let mut atlas = daemon(addr, "atlas").await;
        send_frame(&mut atlas, &ClientFrame::Projects {}).await;
        let first = next_frame(&mut atlas).await;
        assert_eq!(
            first,
            Some(projects_frame(&[("example-repo", &[EXAMPLE_REPO])]))
        );

        // A new grant, and the access cache would miss on it: a new cut would
        // both call GitHub and answer differently. Neither happens.
        app.directory
            .grants
            .lock()
            .unwrap()
            .push(("ghp_atlas".to_owned(), PORTAL.to_owned()));
        let reads = app
            .directory
            .reads_started
            .load(std::sync::atomic::Ordering::SeqCst);
        for _ in 0..5 {
            send_frame(&mut atlas, &ClientFrame::Projects {}).await;
            assert_eq!(next_frame(&mut atlas).await, first);
        }
        assert_eq!(
            app.directory
                .reads_started
                .load(std::sync::atomic::Ordering::SeqCst),
            reads,
            "a repeat inside the cooldown cut the map again"
        );

        // The cooldown is per connection: a new one cuts afresh.
        let mut again = daemon(addr, "atlas").await;
        send_frame(&mut again, &ClientFrame::Projects {}).await;
        assert_eq!(
            next_frame(&mut again).await,
            Some(projects_frame(&[("example-repo", &[EXAMPLE_REPO, PORTAL])]))
        );
    }

    #[tokio::test]
    async fn a_backfill_of_a_room_not_held_is_one_refusal_whether_it_exists_or_not() {
        // Rule 25. Three rooms atlas does not hold: one that is live and
        // someone else's, one with stored history and no members that she
        // could join, and one whose project has never existed. Each answer
        // must be the same bytes apart from the room she named, and none of
        // them may have read the store.
        let store = Arc::new(Fake::new());
        let app = org_app_over(&[("ghp_atlas", SKILLS), ("ghp_bay", EXAMPLE_REPO)], &store);
        let addr = serve(Arc::clone(&app)).await;

        let mut bay = daemon(addr, "bay").await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        join(&mut bay, &example_repo).await;
        let skills = repo_room("example-tools", SKILLS);
        store.insert(stored(&skills, 1, "cedar", "history, no members"));
        let ghost = RoomId::new(
            sealmesh_wire::ProjectId::try_from("ghost").unwrap(),
            Scope::Project,
        );

        let mut atlas = daemon(addr, "atlas").await;
        let asked = [
            // Spelled differently from the router's key: the refusal must
            // echo what was sent, not reveal the map's spelling.
            repo_room("example-repo", "Example-org/Example-repo"),
            skills.clone(),
            ghost,
        ];
        let mut stripped = Vec::new();
        for room in &asked {
            send_frame(&mut atlas, &backfill(room, Some(0))).await;
            let reply = next_frame(&mut atlas).await.unwrap();
            assert_eq!(reply, not_held(room));
            let json = serde_json::to_string(&reply).unwrap();
            let room_json = serde_json::to_string(room).unwrap();
            assert!(json.contains(&room_json), "{json}");
            stripped.push(json.replace(&room_json, ""));
        }
        assert!(
            stripped.len() == 3 && stripped.iter().all(|one| Some(one) == stripped.first()),
            "the refusals differ: {stripped:?}"
        );
        assert_eq!(
            store.page_reads(),
            0,
            "a non-member's backfill read the store"
        );
    }

    #[tokio::test]
    async fn a_member_reads_an_archived_ticket_rooms_history_in_order_from_any_cursor() {
        let store = Arc::new(Fake::new());
        let app = org_app_over(
            &[("ghp_atlas", EXAMPLE_REPO), ("ghp_atlas", PORTAL)],
            &store,
        );
        *app.directory.pulls.lock().unwrap() = Ok(vec![crate::auth::PullSummary {
            head_ref: "feature/eng-12-portal".to_owned(),
            into_default_branch: true,
            state: crate::auth::PullState::Merged,
        }]);
        let addr = serve(Arc::clone(&app)).await;
        let ticket = ticket_room(12);
        for (seq, text) in [(1, "claiming"), (2, "handing off"), (3, "merged")] {
            store.insert(stored(&ticket, seq, "bay", text));
        }
        let mut atlas = daemon(addr, "atlas").await;
        assert_eq!(
            join(&mut atlas, &ticket).await,
            Some(ServerFrame::Joined {
                room: ticket.clone()
            })
        );
        // Closed to writes...
        send_frame(&mut atlas, &post(&ticket, "too late")).await;
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::PostRefused {
                room: ticket.clone(),
                category: PostRefusal::Archived,
            })
        );

        // ...and still readable, whole and from a cursor.
        send_frame(&mut atlas, &backfill(&ticket, None)).await;
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::Backfill {
                room: ticket.clone(),
                after: None,
                messages: vec![
                    entry(1, "bay", "claiming"),
                    entry(2, "bay", "handing off"),
                    entry(3, "bay", "merged"),
                ],
                next: None,
            })
        );
        send_frame(&mut atlas, &backfill(&ticket, Some(1))).await;
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::Backfill {
                room: ticket.clone(),
                after: Some(RoomSeq(1)),
                messages: vec![entry(2, "bay", "handing off"), entry(3, "bay", "merged")],
                next: None,
            })
        );
    }

    #[tokio::test]
    async fn a_backfill_whose_membership_ends_mid_read_answers_not_held_and_nothing_it_read() {
        let store = Arc::new(Fake::new());
        let app = org_app_over(&[("ghp_atlas", EXAMPLE_REPO)], &store);
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        join(&mut atlas, &example_repo).await;
        store.insert(stored(
            &example_repo,
            1,
            "bay",
            "read under a membership that ended",
        ));
        store.set_page_delay(Duration::from_millis(500));

        send_frame(&mut atlas, &backfill(&example_repo, None)).await;
        wait_until("the backfill read starting", || store.page_reads() > 0).await;
        send_frame(
            &mut atlas,
            &ClientFrame::Leave {
                room: example_repo.clone(),
            },
        )
        .await;
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::Left {
                room: example_repo.clone()
            })
        );
        assert_eq!(next_frame(&mut atlas).await, Some(not_held(&example_repo)));
    }

    #[tokio::test]
    async fn an_unreadable_store_refuses_a_members_backfill_as_unavailable() {
        let store = Arc::new(Fake::new());
        let app = org_app_over(&[("ghp_atlas", EXAMPLE_REPO)], &store);
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        join(&mut atlas, &example_repo).await;
        store.fail_reads();

        send_frame(&mut atlas, &backfill(&example_repo, None)).await;
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::BackfillRefused {
                room: example_repo,
                category: BackfillRefusal::Unavailable,
            })
        );
    }

    #[tokio::test]
    async fn a_room_listing_names_only_live_rooms_this_daemons_own_token_can_join() {
        let store = Arc::new(Fake::new());
        let app = org_app_over(
            &[
                ("ghp_atlas", EXAMPLE_REPO),
                ("ghp_atlas", PORTAL),
                ("ghp_bay", EXAMPLE_REPO),
                ("ghp_cedar", SKILLS),
            ],
            &store,
        );
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let skills = repo_room("example-tools", SKILLS);
        let mut bay = daemon(addr, "bay").await;
        join(&mut bay, &example_repo).await;
        let mut cedar = daemon(addr, "cedar").await;
        join(&mut cedar, &skills).await;
        // Stored history atlas could read, with nobody in the room: not live.
        let project = RoomId::new(
            sealmesh_wire::ProjectId::try_from("example-repo").unwrap(),
            Scope::Project,
        );
        store.insert(stored(&project, 1, "bay", "nobody is here now"));

        // atlas holds no rooms of her own; skills is cedar's, and atlas's
        // token cannot read it.
        let mut atlas = daemon(addr, "atlas").await;
        send_frame(&mut atlas, &ClientFrame::ListRooms {}).await;
        assert_eq!(
            next_frame(&mut atlas).await,
            Some(ServerFrame::RoomList {
                rooms: sealmesh_proto::RoomList::new(vec![example_repo.clone()]).unwrap(),
            })
        );

        // cedar, asking with her own token, sees her own room and not bay's.
        send_frame(&mut cedar, &ClientFrame::ListRooms {}).await;
        assert_eq!(
            next_frame(&mut cedar).await,
            Some(ServerFrame::RoomList {
                rooms: sealmesh_proto::RoomList::new(vec![skills]).unwrap(),
            })
        );
        assert_eq!(store.page_reads(), 0, "a listing read room history");
    }

    #[tokio::test]
    async fn a_backfill_does_not_wait_behind_a_post_holding_the_ingest_lock() {
        // The ingest lock is process-wide and held across a post's write. A
        // read has nothing to order against, so it must not queue behind
        // another host's slow write — deterministic the same way as the
        // query tests: the backfill is sent only once the write has provably
        // started, and must be answered in a fraction of it.
        const WRITE: Duration = Duration::from_secs(2);
        const WHILE_WRITING: Duration = Duration::from_millis(300);

        let store = Arc::new(Fake::slow(WRITE));
        let app = org_app_over(
            &[("ghp_atlas", EXAMPLE_REPO), ("ghp_bay", EXAMPLE_REPO)],
            &store,
        );
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        let mut bay = daemon(addr, "bay").await;
        join(&mut atlas, &example_repo).await;
        join(&mut bay, &example_repo).await;
        store.insert(stored(&example_repo, 1, "cedar", "already recorded"));

        send_frame(&mut bay, &post(&example_repo, "slow to record")).await;
        wait_until("bay's write starting", || store.appends() > 0).await;

        send_frame(&mut atlas, &backfill(&example_repo, None)).await;
        match tokio::time::timeout(WHILE_WRITING, next_frame(&mut atlas)).await {
            Ok(Some(ServerFrame::Backfill { messages, .. })) => {
                assert_eq!(messages, vec![entry(1, "cedar", "already recorded")]);
            }
            other => panic!("the backfill waited behind another host's post: {other:?}"),
        }
        assert_eq!(message_body(next_frame(&mut atlas).await), "slow to record");
    }

    #[tokio::test]
    async fn a_reader_can_neither_backfill_nor_list_rooms() {
        let store = Arc::new(Fake::new());
        let app = org_app_over(&[("ghp_atlas", EXAMPLE_REPO)], &store);
        let addr = serve(Arc::clone(&app)).await;
        let example_repo = repo_room("example-repo", EXAMPLE_REPO);
        let mut atlas = daemon(addr, "atlas").await;
        join(&mut atlas, &example_repo).await;
        store.insert(stored(&example_repo, 1, "atlas", "daemons only"));

        let mut reader = open(addr).await;
        send_frame(
            &mut reader,
            &ClientFrame::Hello {
                role: Role::Reader,
                token: SecretToken::new("ghp_atlas".to_owned()),
            },
        )
        .await;
        assert!(matches!(
            next_frame(&mut reader).await,
            Some(ServerFrame::Welcome { .. })
        ));
        send_frame(&mut reader, &backfill(&example_repo, None)).await;
        send_frame(&mut reader, &ClientFrame::ListRooms {}).await;
        assert!(quiet_for(&mut reader, Duration::from_millis(200)).await);
        assert_eq!(store.page_reads(), 0);
    }
}
