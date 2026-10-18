use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use jiff::Timestamp;
use sealmesh_proto::{ClientFrame, EventBatch, HostState, MAX_EVENTS_PER_BATCH, Role, ServerFrame};
use sealmesh_wire::HostId;
use tokio::sync::mpsc;

use crate::{
    audit,
    client::{self, Connected, Socket},
    collect::{self, git::GitResolver, roots},
    config::Config,
    events::Watch,
    room::{
        self, Command, Gate, Shared,
        client::{RoomClient, Timing},
        inbox::{Limits, State},
        server::{self as local_server, Server},
        socket,
    },
    wire,
};

/// How often the session checks its room deadlines, the traffic interval and
/// whether chat has been turned off. Only bookkeeping runs on it: it never
/// lists rooms or asks the router anything.
const ROOM_TICK: Duration = Duration::from_secs(1);

const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

const HEARTBEAT: Duration = Duration::from_secs(30);

const QUERY_BURST: u32 = 10;

/// How long a daemon takes to earn back one query after its burst is spent.
/// Public because `sealmesh-mcp` derives the floor under its own
/// `fleet_status` cooldown from it, so the two cannot drift apart.
pub const QUERY_REFILL: Duration = Duration::from_secs(6);

async fn within<T>(
    limit: Duration,
    step: impl Future<Output = Result<T>>,
    what: &str,
) -> Result<T> {
    match tokio::time::timeout(limit, step).await {
        Ok(result) => result,
        Err(_) => {
            bail!("the {what} write did not complete in {limit:?}, the connection is stalled")
        }
    }
}

const SILENCE_LIMIT: Duration = Duration::from_secs(90);

const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

const EMIT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub struct Link {
    last_heard: Instant,
    limit: Duration,
}

impl Link {
    pub fn new(limit: Duration, now: Instant) -> Self {
        Self {
            last_heard: now,
            limit,
        }
    }

    pub fn standard(now: Instant) -> Self {
        Self::new(SILENCE_LIMIT, now)
    }

    pub fn heard(&mut self, now: Instant) {
        self.last_heard = self.last_heard.max(now);
    }

    pub fn is_silent(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.last_heard) > self.limit
    }
}

#[derive(Debug)]
pub struct QueryBudget {
    tokens: u32,
    capacity: u32,
    refill: Duration,
    filled_to: Instant,
}

impl QueryBudget {
    pub fn new(capacity: u32, refill: Duration, now: Instant) -> Self {
        Self {
            tokens: capacity,
            capacity,
            refill,
            filled_to: now,
        }
    }

    pub fn standard(now: Instant) -> Self {
        Self::new(QUERY_BURST, QUERY_REFILL, now)
    }

    pub fn allow(&mut self, now: Instant) -> bool {
        self.earn(now);
        if self.tokens == 0 {
            return false;
        }
        self.tokens = self.tokens.saturating_sub(1);
        true
    }

    fn earn(&mut self, now: Instant) {
        let interval = self.refill.as_nanos();
        if interval == 0 {
            self.tokens = self.capacity;
            self.filled_to = now;
            return;
        }

        let elapsed = now.saturating_duration_since(self.filled_to).as_nanos();
        let earned = elapsed / interval;
        if earned == 0 {
            return;
        }

        self.tokens = self
            .tokens
            .saturating_add(u32::try_from(earned).unwrap_or(u32::MAX))
            .min(self.capacity);

        let consumed = earned.saturating_mul(interval);
        let consumed = Duration::from_nanos(u64::try_from(consumed).unwrap_or(u64::MAX));
        self.filled_to = self.filled_to.checked_add(consumed).unwrap_or(now);
    }
}

fn collect_now(config: &Config) -> Result<Vec<wire::Agent>> {
    let gate = config.gate()?;
    let options = config.collect_options();
    let providers = collect::providers::with_configured(
        roots::discover_local(&config.claude.extra_roots),
        options.declared_dir.clone(),
        options.declared_ttl,
        &config.provider,
    );
    let resolver = GitResolver::new(config.integration_branch.clone());
    Ok(collect::collect(&providers, &gate, &resolver, &options))
}

pub fn state_now(config: &Config, host: &HostId, at: Timestamp) -> Result<HostState> {
    if is_off() {
        return Ok(HostState::NotSharing {});
    }
    let agents = collect_now(config)?;

    Ok(HostState::Answering {
        snapshot: Box::new(collect::snapshot(host.clone(), at, agents)),
    })
}

pub fn is_off() -> bool {
    Config::off_switch_path().is_some_and(|p| p.exists())
}

/// The session's half of room chat: the room client, and the receiving end of
/// the bounded channel the local socket server hands it requests on. Lives as
/// long as the daemon, across reconnects, so membership survives one.
pub struct Rooms {
    pub client: RoomClient,
    pub commands: mpsc::Receiver<Command>,
}

/// Starts the local socket server and returns the session's half. A socket
/// that cannot be bound — a path too long, another daemon already serving, a
/// file in the way — leaves the daemon answering `who` as before, with no
/// local room access, and says why.
fn start_rooms(config: &Config) -> Rooms {
    let shared = Shared::new(State::new(Limits::default(), Timestamp::now()));
    let project_rooms = config.chat.project_rooms;
    let config = Arc::new(config.clone());
    let gate: Gate = Arc::new(move || config.chat_enabled(is_off()));
    let (commands_tx, commands) = room::commands();

    match socket::default_path()
        .ok_or_else(|| anyhow::anyhow!("no home directory to place the local socket in"))
        .and_then(|path| listen(&path))
    {
        Ok((listener, uid)) => {
            let server = Arc::new(Server {
                shared: shared.clone(),
                commands: commands_tx,
                gate: gate.clone(),
                uid,
                limits: local_server::Limits::default(),
                stats: Arc::default(),
            });
            tokio::spawn(local_server::serve(listener, server));
        }
        Err(error) => eprintln!("room chat has no local socket: {error:#}"),
    }

    Rooms {
        client: RoomClient::new(
            shared,
            gate,
            Timing::default(),
            audit::path(),
            Instant::now(),
        )
        .with_project_rooms(project_rooms),
        commands,
    }
}

fn listen(path: &std::path::Path) -> Result<(tokio::net::UnixListener, u32)> {
    let (listener, uid) = socket::bind(path)?;
    listener
        .set_nonblocking(true)
        .context("making the local socket non-blocking")?;
    let listener =
        tokio::net::UnixListener::from_std(listener).context("registering the local socket")?;
    Ok((listener, uid))
}

pub async fn run(config: &Config) -> Result<()> {
    let mut backoff = BACKOFF_MIN;
    let mut watch = Watch::new();
    let mut rooms = start_rooms(config);

    loop {
        match session(config, &mut watch, &mut rooms).await {
            Ok(()) => {
                eprintln!("router closed the connection, reconnecting");
                backoff = BACKOFF_MIN;
            }
            Err(error) => {
                eprintln!("disconnected, retrying in {backoff:?}: {error:#}");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

async fn session(config: &Config, watch: &mut Watch, rooms: &mut Rooms) -> Result<()> {
    let Connected { socket, host } = client::connect(&config.server, Role::Daemon).await?;
    eprintln!("connected as {host}");
    let collect = |host: &HostId| match state_now(config, host, Timestamp::now()) {
        Ok(state) => state,
        Err(error) => {
            eprintln!("collect failed, answering unreachable: {error:#}");
            HostState::Unreachable {}
        }
    };
    serve(socket, host, config, watch, rooms, collect).await
}

/// Runs one router connection until it ends. `collect` answers a query that
/// the budget allowed.
///
/// This loop is the only place frames are read and written. Room work reaches
/// it as a command from the local socket server or as a frame from the router,
/// and `RoomClient` handles either without waiting on anything, so the only
/// I/O in any arm is this socket's own writes, each under `WRITE_TIMEOUT`.
/// That is the guarantee, and a test pins it. `biased` adds only a preference:
/// it polls the router's socket first on each turn of the loop, so a `Query`
/// that has already arrived tends to be read ahead of queued local requests.
/// No test pins that ordering.
async fn serve(
    mut socket: Socket,
    host: HostId,
    config: &Config,
    watch: &mut Watch,
    rooms: &mut Rooms,
    collect: impl Fn(&HostId) -> HostState,
) -> Result<()> {
    let rejoin = rooms.client.on_connect(host.clone(), Instant::now());
    let result = async {
        send_all(&mut socket, rejoin).await?;
        serve_connected(&mut socket, &host, config, watch, rooms, collect).await
    }
    .await;
    rooms.client.on_disconnect();
    result
}

async fn send_all(socket: &mut Socket, frames: Vec<ClientFrame>) -> Result<()> {
    for frame in frames {
        within(WRITE_TIMEOUT, client::send(socket, &frame), "room").await?;
    }
    Ok(())
}

async fn serve_connected(
    socket: &mut Socket,
    host: &HostId,
    config: &Config,
    watch: &mut Watch,
    rooms: &mut Rooms,
    collect: impl Fn(&HostId) -> HostState,
) -> Result<()> {
    let mut budget = QueryBudget::standard(Instant::now());

    let mut link = Link::standard(Instant::now());

    let mut heartbeat = tokio::time::interval(HEARTBEAT);
    heartbeat.tick().await;

    let mut events_tick = tokio::time::interval(config.events.interval());
    events_tick.tick().await;

    let mut room_tick = tokio::time::interval(ROOM_TICK);
    room_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    room_tick.tick().await;

    loop {
        let frame = tokio::select! {
            biased;
            incoming = client::recv(socket) => {
                link.heard(Instant::now());
                match incoming? {
                    client::Incoming::Frame(frame) => frame,
                    client::Incoming::Noise => continue,
                    // A frame this build cannot read is not a reason to drop a
                    // working connection. Waiting for the next one costs
                    // nothing; tearing the socket down makes this machine read
                    // as unreachable, which looks exactly like a closed laptop.
                    client::Incoming::Undecodable => continue,
                    client::Incoming::UnknownRefusal(refusal) => {
                        let frames = rooms.client.on_unknown_refusal(refusal);
                        send_all(socket, frames).await?;
                        continue;
                    }
                }
            }
            _ = heartbeat.tick() => {
                if link.is_silent(Instant::now()) {
                    bail!(
                        "the router has not answered a heartbeat in {SILENCE_LIMIT:?}, \
                         treating the connection as dead"
                    );
                }
                within(WRITE_TIMEOUT, client::ping(socket), "heartbeat").await?;
                continue;
            }
            _ = events_tick.tick() => {
                within(EMIT_TIMEOUT, emit(config, socket, host, watch), "events").await?;
                continue;
            }
            Some(command) = rooms.commands.recv() => {
                let frames = rooms.client.on_command(command, Instant::now());
                send_all(socket, frames).await?;
                continue;
            }
            _ = room_tick.tick() => {
                let frames = rooms.client.on_tick(Instant::now());
                send_all(socket, frames).await?;
                continue;
            }
        };

        match frame {
            ServerFrame::Query { id, asked_by } => {
                let (state, refused) = answer_query(budget.allow(Instant::now()), || collect(host));
                if refused {
                    eprintln!(
                        "refusing to collect: more queries than this machine answers in a \
                         burst. See `sealmesh audit`."
                    );
                }
                note(&state, &asked_by, refused);
                within(
                    WRITE_TIMEOUT,
                    client::send(socket, &ClientFrame::Answer { id, state }),
                    "answer",
                )
                .await?;
            }
            other => {
                let frames = rooms.client.on_frame(other, Instant::now());
                send_all(socket, frames).await?;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tick {
    Disabled,
    StandDown,
    Collect,
}

fn tick_action(enabled: bool, sharing_off: bool) -> Tick {
    if !enabled {
        return Tick::Disabled;
    }
    if sharing_off {
        return Tick::StandDown;
    }
    Tick::Collect
}

async fn emit(
    config: &Config,
    socket: &mut Socket,
    host: &HostId,
    watch: &mut Watch,
) -> Result<()> {
    match tick_action(config.events.enabled, is_off()) {
        Tick::Disabled => return Ok(()),
        Tick::StandDown => {
            watch.forget();
            return Ok(());
        }
        Tick::Collect => {}
    }

    let agents = match collect_now(config) {
        Ok(agents) => agents,
        Err(error) => {
            eprintln!("collect failed, emitting no events this interval: {error:#}");
            return Ok(());
        }
    };

    let events = watch.observe(agents, host, Timestamp::now());
    for chunk in events.chunks(MAX_EVENTS_PER_BATCH) {
        let batch = EventBatch::new(chunk.to_vec()).context("building an event batch")?;
        client::send(socket, &ClientFrame::Events { batch }).await?;
    }
    Ok(())
}

/// What this machine answers one query with, and whether its budget refused
/// the query. `allowed` is the budget's verdict; `collect` runs only when it
/// allowed, so a refusal costs no collection.
///
/// A refusal answers `Throttled`, never `Unreachable`: a caller asking too
/// often must not make a working machine read as a closed laptop to everyone
/// else who asks.
fn answer_query(allowed: bool, collect: impl FnOnce() -> HostState) -> (HostState, bool) {
    if allowed {
        (collect(), false)
    } else {
        (HostState::Throttled {}, true)
    }
}

/// The audit ledger's record of one query. A refused query is recorded too
/// (rule 28), so a flood is visible to the person being flooded.
fn audit_entry(state: &HostState, asked_by: &HostId, refused: bool, at: Timestamp) -> audit::Entry {
    audit::Entry {
        at,
        asked_by: asked_by.clone(),
        agents: match state {
            HostState::Answering { snapshot } => Some(snapshot.agents.len()),
            HostState::NotSharing {} | HostState::Unreachable {} | HostState::Throttled {} => None,
        },
        refused,
    }
}

fn note(state: &HostState, asked_by: &HostId, refused: bool) {
    let Some(path) = audit::path() else { return };
    let entry = audit_entry(state, asked_by, refused, Timestamp::now());
    if let Err(error) = audit::record(&path, &entry) {
        eprintln!("could not write the audit log: {error:#}");
    }
}

pub fn dry_run(config: &Config) -> Result<()> {
    let host = HostId::try_from("dry-run").context("building the placeholder host")?;
    let state = state_now(config, &host, Timestamp::now())?;

    match state {
        HostState::Answering { snapshot } => {
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        HostState::NotSharing {} => println!("sharing is off — this machine would answer nothing"),
        HostState::Unreachable {} => println!("this machine could not read its own state"),
        // `state_now` never throttles; only a query from the router can.
        HostState::Throttled {} => println!("this machine is refusing queries for now"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn a_router_that_goes_quiet_past_the_limit_is_dead() {
        let t0 = Instant::now();
        let link = Link::standard(t0);

        assert!(!link.is_silent(t0), "a fresh connection read as dead");
        assert!(
            !link.is_silent(t0 + SILENCE_LIMIT),
            "the limit itself is not past it"
        );
        assert!(link.is_silent(t0 + SILENCE_LIMIT + Duration::from_secs(1)));
    }

    #[test]
    fn a_link_that_only_ever_hears_pongs_stays_alive_indefinitely() {
        let t0 = Instant::now();
        let mut link = Link::standard(t0);

        for beat in 1..=960 {
            let now = t0 + HEARTBEAT * beat;
            assert!(
                !link.is_silent(now),
                "reconnected on a healthy idle link at beat {beat}"
            );
            link.heard(now);
        }
    }

    #[test]
    fn a_single_missed_heartbeat_does_not_reconnect() {
        let t0 = Instant::now();
        let link = Link::standard(t0);

        assert!(
            !link.is_silent(t0 + HEARTBEAT * 2),
            "one miss triggered a reconnect"
        );
        assert!(
            !link.is_silent(t0 + HEARTBEAT * 3),
            "two misses triggered a reconnect"
        );
    }

    #[test]
    fn the_silence_limit_is_a_multiple_of_the_heartbeat() {
        assert_eq!(
            SILENCE_LIMIT.as_secs() % HEARTBEAT.as_secs(),
            0,
            "{SILENCE_LIMIT:?} is not a whole number of {HEARTBEAT:?} beats"
        );
        assert!(
            SILENCE_LIMIT >= HEARTBEAT * 3,
            "{SILENCE_LIMIT:?} would reconnect on fewer than three missed beats"
        );
    }

    #[test]
    fn any_traffic_resets_the_clock() {
        let t0 = Instant::now();
        let mut link = Link::standard(t0);

        let nearly_dead = t0 + SILENCE_LIMIT;
        assert!(!link.is_silent(nearly_dead));
        link.heard(nearly_dead);
        assert!(!link.is_silent(nearly_dead + SILENCE_LIMIT));
        assert!(link.is_silent(nearly_dead + SILENCE_LIMIT + Duration::from_secs(1)));
    }

    #[test]
    fn an_out_of_order_stamp_never_ages_the_link() {
        let t0 = Instant::now();
        let mut link = Link::standard(t0 + SILENCE_LIMIT);
        link.heard(t0);
        assert!(!link.is_silent(t0 + SILENCE_LIMIT));
    }

    #[test]
    fn the_write_deadlines_are_bounded_and_ordered() {
        assert!(WRITE_TIMEOUT >= Duration::from_secs(5), "{WRITE_TIMEOUT:?}");
        assert!(
            WRITE_TIMEOUT < SILENCE_LIMIT,
            "a stalled write outlives the link check"
        );
        assert!(EMIT_TIMEOUT > WRITE_TIMEOUT, "{EMIT_TIMEOUT:?}");
        assert!(EMIT_TIMEOUT <= Duration::from_secs(300), "{EMIT_TIMEOUT:?}");
    }

    #[tokio::test]
    async fn a_write_that_never_completes_becomes_an_error() {
        let started = Instant::now();
        let never = std::future::pending::<Result<()>>();
        let result = within(Duration::from_millis(50), never, "test").await;

        assert!(result.is_err(), "a hung write was not caught");
        let message = format!("{:#}", result.unwrap_err());
        assert!(message.contains("stalled"), "{message}");
        assert!(
            message.contains("test"),
            "which write blew it is not named: {message}"
        );
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn a_write_that_completes_in_time_is_kept() {
        let result = within(Duration::from_secs(30), async { Ok(7) }, "test").await;
        assert_eq!(result.unwrap(), 7);
    }

    #[test]
    fn backoff_doubles_up_to_a_ceiling() {
        let mut backoff = BACKOFF_MIN;
        let mut seen = Vec::new();
        for _ in 0..12 {
            seen.push(backoff);
            backoff = (backoff * 2).min(BACKOFF_MAX);
        }
        assert_eq!(seen.first(), Some(&BACKOFF_MIN));
        assert_eq!(backoff, BACKOFF_MAX);
        assert!(
            seen.windows(2)
                .all(|w| matches!((w.first(), w.last()), (Some(a), Some(b)) if a <= b)),
            "{seen:?}"
        );
    }

    #[test]
    fn the_heartbeat_fits_comfortably_inside_the_load_balancer_timeout() {
        const ALB_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
        assert!(
            HEARTBEAT * 2 <= ALB_IDLE_TIMEOUT,
            "one lost heartbeat must not reach the timeout: {HEARTBEAT:?}"
        );
    }

    #[test]
    fn a_flood_of_queries_stops_costing_collections() {
        let t0 = Instant::now();
        let mut budget = QueryBudget::new(QUERY_BURST, QUERY_REFILL, t0);

        let allowed = (0..1000).filter(|_| budget.allow(t0)).count();
        assert_eq!(
            allowed,
            usize::try_from(QUERY_BURST).unwrap(),
            "a thousand queries in one instant must cost at most the burst"
        );
    }

    #[test]
    fn waiting_earns_the_budget_back_at_the_configured_rate() {
        let t0 = Instant::now();
        let mut budget = QueryBudget::new(QUERY_BURST, QUERY_REFILL, t0);
        while budget.allow(t0) {}
        assert!(!budget.allow(t0), "the burst was not spent");

        assert!(
            !budget.allow(t0 + QUERY_REFILL / 2),
            "half an interval must not buy a whole collection"
        );
        assert!(budget.allow(t0 + QUERY_REFILL), "one interval buys one");
        assert!(!budget.allow(t0 + QUERY_REFILL), "and only one");
    }

    #[test]
    fn a_remainder_below_the_interval_is_carried_rather_than_discarded() {
        let t0 = Instant::now();
        let mut budget = QueryBudget::new(1, Duration::from_secs(10), t0);
        assert!(budget.allow(t0));
        assert!(!budget.allow(t0));

        assert!(!budget.allow(t0 + Duration::from_secs(6)));
        assert!(
            budget.allow(t0 + Duration::from_secs(12)),
            "two sub-interval waits earned nothing"
        );
    }

    #[test]
    fn an_idle_daemon_does_not_bank_more_than_the_burst() {
        let t0 = Instant::now();
        let mut budget = QueryBudget::new(QUERY_BURST, QUERY_REFILL, t0);
        while budget.allow(t0) {}

        let much_later = t0 + Duration::from_secs(60 * 60 * 24);
        let allowed = (0..1000).filter(|_| budget.allow(much_later)).count();
        assert_eq!(allowed, usize::try_from(QUERY_BURST).unwrap());
    }

    #[test]
    fn a_person_running_who_repeatedly_is_never_refused() {
        let t0 = Instant::now();
        let mut budget = QueryBudget::standard(t0);
        for i in 0..20 {
            let at = t0 + Duration::from_secs(i * 10);
            assert!(budget.allow(at), "refused a human at invocation {i}");
        }
    }

    #[test]
    fn a_refusal_is_recorded_and_reads_differently_from_an_answer() {
        let entry = |refused, agents| audit::Entry {
            at: "2026-08-13T12:00:00Z".parse().unwrap(),
            asked_by: HostId::try_from("shelter").unwrap(),
            agents,
            refused,
        };
        let refused = serde_json::to_string(&entry(true, None)).unwrap();
        let answered = serde_json::to_string(&entry(false, Some(0))).unwrap();

        assert!(refused.contains(r#""refused":true"#), "{refused}");
        assert_ne!(
            refused, answered,
            "a throttled read must not serialise the same as an answered one"
        );
    }

    #[test]
    fn the_off_switch_decides_an_event_tick_before_anything_is_read() {
        assert_eq!(tick_action(true, true), Tick::StandDown);
        assert_eq!(tick_action(true, false), Tick::Collect);
        assert_eq!(tick_action(false, false), Tick::Disabled);
        assert_eq!(tick_action(false, true), Tick::Disabled);
    }

    #[test]
    fn more_events_than_one_frame_holds_are_split_rather_than_truncated() {
        let events: Vec<usize> = (0..MAX_EVENTS_PER_BATCH * 2 + 1).collect();
        let batches: Vec<&[usize]> = events.chunks(MAX_EVENTS_PER_BATCH).collect();

        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|b| b.len() <= MAX_EVENTS_PER_BATCH));
        let flattened: Vec<usize> = batches.concat();
        assert_eq!(flattened, events, "an event was dropped by the split");
    }

    #[test]
    fn the_event_interval_is_nothing_like_the_heartbeat() {
        let default = crate::config::EventsConfig::default().interval();
        assert!(
            default >= HEARTBEAT * 2,
            "collection would run at heartbeat rate: {default:?}"
        );
    }

    #[test]
    fn not_sharing_and_unreachable_produce_different_audit_entries_from_answering() {
        let answering = HostState::Answering {
            snapshot: Box::new(collect::snapshot(
                HostId::try_from("sandbank").unwrap(),
                "2026-08-13T12:00:00Z".parse().unwrap(),
                Vec::new(),
            )),
        };
        let asked_by = HostId::try_from("shelter").unwrap();
        let at: Timestamp = "2026-08-13T12:00:00Z".parse().unwrap();
        let count = |state: &HostState| audit_entry(state, &asked_by, false, at).agents;
        assert_eq!(count(&answering), Some(0));
        assert_eq!(count(&HostState::NotSharing {}), None);
        assert_eq!(count(&HostState::Unreachable {}), None);
    }

    #[test]
    fn a_refused_query_answers_throttled_and_never_collects() {
        let collected = std::cell::Cell::new(0);
        let collect = || {
            collected.set(collected.get() + 1);
            HostState::NotSharing {}
        };

        let (state, refused) = answer_query(false, collect);
        assert_eq!(state, HostState::Throttled {});
        assert!(refused);
        assert_eq!(collected.get(), 0, "a refusal paid for a collection");

        let (state, refused) = answer_query(true, collect);
        assert_eq!(state, HostState::NotSharing {});
        assert!(!refused);
        assert_eq!(collected.get(), 1);
    }

    #[test]
    fn a_drained_budget_answers_throttled_never_unreachable() {
        let t0 = Instant::now();
        let mut budget = QueryBudget::standard(t0);
        let answers: Vec<HostState> = (0..=QUERY_BURST)
            .map(|_| answer_query(budget.allow(t0), || HostState::NotSharing {}).0)
            .collect();

        let (last, burst) = answers.split_last().unwrap();
        assert!(burst.iter().all(|state| *state == HostState::NotSharing {}));
        assert_eq!(*last, HostState::Throttled {});
        assert!(!answers.contains(&HostState::Unreachable {}), "{answers:?}");
    }

    #[test]
    fn a_throttled_answer_is_still_recorded_as_a_refusal_in_the_audit_ledger() {
        let entry = audit_entry(
            &HostState::Throttled {},
            &HostId::try_from("shelter").unwrap(),
            true,
            "2026-08-13T12:00:00Z".parse().unwrap(),
        );
        assert!(entry.refused);
        assert_eq!(entry.agents, None);
        assert_eq!(entry.asked_by.as_str(), "shelter");
        let line = serde_json::to_string(&entry).unwrap();
        assert!(line.contains(r#""refused":true"#), "{line}");
    }

    /// A router connection end to end: a real WebSocket, a real local socket
    /// server, and the daemon's own `serve` loop between them.
    mod session {
        #![allow(
            clippy::unwrap_used,
            clippy::expect_used,
            clippy::panic,
            clippy::indexing_slicing
        )]

        use std::{
            path::PathBuf,
            sync::atomic::{AtomicBool, AtomicU64, Ordering},
        };

        use futures_util::{SinkExt as _, StreamExt as _};
        use sealmesh_chat_wire::{Body, MessageKind, RoomId, Scope};
        use sealmesh_proto::QueryId;
        use sealmesh_wire::{ProjectId, RepoSlug, TicketId};
        use tokio::{
            io::AsyncWriteExt as _,
            net::{TcpListener, TcpStream, UnixStream},
            sync::mpsc,
        };
        use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

        use super::*;
        use crate::room::{
            connect,
            local::{JoinOutcome, Request, ResolvedRoom, Response},
            server::Stats,
        };

        struct FakeRouter {
            frames: mpsc::UnboundedReceiver<ClientFrame>,
            out: mpsc::UnboundedSender<Outgoing>,
            task: tokio::task::JoinHandle<()>,
            /// Project map requests `next` skipped over.
            projects_asked: u64,
        }

        impl FakeRouter {
            fn spawn(mut ws: WebSocketStream<TcpStream>) -> Self {
                let (frames_tx, frames) = mpsc::unbounded_channel();
                let (out, mut out_rx) = mpsc::unbounded_channel::<Outgoing>();
                let task = tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            message = ws.next() => {
                                let Some(Ok(message)) = message else { break };
                                if let Message::Text(text) = message {
                                    let frame = sealmesh_proto::decode(text.as_bytes()).unwrap();
                                    if frames_tx.send(frame).is_err() {
                                        break;
                                    }
                                }
                            }
                            frame = out_rx.recv() => {
                                let Some(frame) = frame else {
                                    let _ = ws.close(None).await;
                                    break;
                                };
                                let text = match frame {
                                    Outgoing::Frame(frame) => serde_json::to_string(&frame).unwrap(),
                                    Outgoing::Raw(text) => text,
                                };
                                ws.send(Message::Text(text.into())).await.unwrap();
                            }
                        }
                    }
                });
                Self {
                    frames,
                    out,
                    task,
                    projects_asked: 0,
                }
            }

            /// The next frame the daemon sent, whatever it is.
            async fn next_any(&mut self) -> ClientFrame {
                tokio::time::timeout(Duration::from_secs(30), self.frames.recv())
                    .await
                    .expect("the daemon sent nothing")
                    .expect("the daemon hung up")
            }

            /// The next frame other than a project map request, which a
            /// daemon sends on every connection and which this router, like
            /// an older one, leaves unanswered unless a test answers it.
            async fn next(&mut self) -> ClientFrame {
                loop {
                    match self.next_any().await {
                        ClientFrame::Projects {} => self.projects_asked += 1,
                        frame => return frame,
                    }
                }
            }

            fn send(&self, frame: ServerFrame) {
                self.out.send(Outgoing::Frame(frame)).unwrap();
            }

            /// Sends text no `ServerFrame` in this build can encode: what a
            /// newer router would send.
            fn send_raw(&self, text: String) {
                self.out.send(Outgoing::Raw(text)).unwrap();
            }
        }

        enum Outgoing {
            Frame(ServerFrame),
            Raw(String),
        }

        async fn connection() -> (Socket, WebSocketStream<TcpStream>) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let accept = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                tokio_tungstenite::accept_async(stream).await.unwrap()
            });
            let (socket, _) =
                tokio_tungstenite::connect_async_with_config(format!("ws://{addr}"), None, false)
                    .await
                    .unwrap();
            (socket, accept.await.unwrap())
        }

        struct Local {
            path: PathBuf,
            root: PathBuf,
            stats: Arc<Stats>,
        }

        impl Drop for Local {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.root);
            }
        }

        fn rooms(tag: &str, timing: Timing, on: Arc<AtomicBool>) -> (Rooms, Local) {
            let root =
                std::env::temp_dir().join(format!("awd{}{tag}", std::process::id() % 100_000));
            let _ = std::fs::remove_dir_all(&root);
            let path = root.join("local").join("daemon.sock");
            let (listener, uid) = listen(&path).unwrap();
            let shared = Shared::new(State::new(Limits::default(), Timestamp::now()));
            let gate: Gate = Arc::new(move || on.load(Ordering::SeqCst));
            let (commands_tx, commands) = room::commands();
            let stats = Arc::new(Stats::default());
            let server = Arc::new(Server {
                shared: shared.clone(),
                commands: commands_tx,
                gate: gate.clone(),
                uid,
                limits: local_server::Limits {
                    answer_deadline: timing.router_wait + Duration::from_secs(10),
                    ..local_server::Limits::default()
                },
                stats: stats.clone(),
            });
            tokio::spawn(local_server::serve(listener, server));
            let client = RoomClient::new(shared, gate, timing, None, Instant::now());
            (Rooms { client, commands }, Local { path, root, stats })
        }

        fn config(root: &std::path::Path) -> Config {
            let toml = format!(
                r#"
                ticket_prefixes = ["ENG"]
                [server]
                url = "http://127.0.0.1:1"
                [allow]
                repos = ["example-org/example-repo"]
                paths = ["{}"]
                [events]
                enabled = false
                "#,
                root.display()
            );
            toml::from_str(&toml).unwrap()
        }

        fn host(name: &str) -> HostId {
            HostId::try_from(name).unwrap()
        }

        fn ticket(n: u32) -> RoomId {
            RoomId::new(
                ProjectId::try_from("example-repo").unwrap(),
                Scope::Ticket(TicketId::new("ENG", n).unwrap()),
            )
        }

        /// Runs `serve` for every connection handed to it, in order, with one
        /// `Rooms` across all of them, the way `run` does across reconnects.
        fn daemon(mut rooms: Rooms, config: Config) -> mpsc::UnboundedSender<Socket> {
            let (tx, mut sockets) = mpsc::unbounded_channel::<Socket>();
            tokio::spawn(async move {
                let mut watch = Watch::new();
                while let Some(socket) = sockets.recv().await {
                    let _ = serve(
                        socket,
                        host("sandbank"),
                        &config,
                        &mut watch,
                        &mut rooms,
                        |_| HostState::NotSharing {},
                    )
                    .await;
                }
            });
            tx
        }

        async fn ask(local: &Local, request: Request) -> Response {
            connect::request(&local.path, &request, Duration::from_secs(60))
                .await
                .unwrap()
        }

        async fn wait_for(counter: &AtomicU64, at_least: u64) {
            let started = std::time::Instant::now();
            while counter.load(Ordering::SeqCst) < at_least {
                assert!(
                    started.elapsed() < Duration::from_secs(10),
                    "never reached {at_least}: {}",
                    counter.load(Ordering::SeqCst)
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        async fn join(local: &Local, router: &mut FakeRouter, room: RoomId) {
            let path = local.path.clone();
            let request = Request::Join {
                room: room.clone(),
                holder: None,
            };
            let joining = tokio::spawn(async move {
                connect::request(&path, &request, Duration::from_secs(60)).await
            });
            assert_eq!(
                router.next().await,
                ClientFrame::Join { room: room.clone() }
            );
            router.send(ServerFrame::Joined { room: room.clone() });
            assert_eq!(joining.await.unwrap().unwrap(), Response::Joined { room });
        }

        /// How long the query below may take. The stalls it runs under last
        /// `STALL`, so a daemon that waited on any of them would miss this by
        /// a wide margin rather than a scheduling hiccup.
        const PROMPT: Duration = Duration::from_secs(2);
        const STALL: Duration = Duration::from_secs(60);

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_stalled_or_flooding_local_client_never_delays_the_answer_to_a_query() {
            let timing = Timing {
                router_wait: STALL,
                post_wait: STALL,
                ..Timing::default()
            };
            let (rooms, local) = rooms("q", timing, Arc::new(AtomicBool::new(true)));
            let (socket, ws) = connection().await;
            let mut router = FakeRouter::spawn(ws);
            let daemon = daemon(rooms, config(&local.root));
            daemon.send(socket).unwrap();

            join(&local, &mut router, ticket(1)).await;

            // Stall one: a backfill the router reads and never answers.
            let path = local.path.clone();
            let _backfill = tokio::spawn(async move {
                connect::request(
                    &path,
                    &Request::Backfill {
                        room: ticket(1),
                        after: None,
                    },
                    STALL,
                )
                .await
            });
            assert_eq!(
                router.next().await,
                ClientFrame::Backfill {
                    room: ticket(1),
                    after: None
                }
            );
            wait_for(&local.stats.awaiting_router, 1).await;

            // Stall two: a client that sends half a request and stops.
            let mut half = UnixStream::connect(&local.path).await.unwrap();
            half.write_all(br#"{"op":"po"#).await.unwrap();
            wait_for(&local.stats.reading, 1).await;

            // A flood: posts as fast as twelve clients can send them, none of
            // them ever confirmed by the router. One is in flight, seven wait
            // queued behind it, and the other four are turned away and go
            // again.
            let sent = Arc::new(AtomicU64::new(0));
            let mut flood = Vec::new();
            for _ in 0..12 {
                let path = local.path.clone();
                let sent = sent.clone();
                flood.push(tokio::spawn(async move {
                    loop {
                        let _ = connect::request(
                            &path,
                            &Request::Post {
                                room: ticket(1),
                                kind: MessageKind::Claim,
                                body: Body::new("flood").unwrap(),
                                consumer: None,
                            },
                            STALL,
                        )
                        .await;
                        sent.fetch_add(1, Ordering::SeqCst);
                        tokio::task::yield_now().await;
                    }
                }));
            }
            // The router has the one post in flight in this room, and the
            // rest of the flood is queued or turned away locally.
            assert!(matches!(router.next().await, ClientFrame::Post { .. }));
            wait_for(&sent, 200).await;
            assert!(
                local.stats.reading.load(Ordering::SeqCst) >= 1,
                "stall two ended"
            );
            assert!(
                local.stats.awaiting_router.load(Ordering::SeqCst) >= 1,
                "stall one ended"
            );

            let asked = std::time::Instant::now();
            router.send(ServerFrame::Query {
                id: QueryId(1),
                asked_by: host("shelter"),
            });
            let answer = tokio::time::timeout(PROMPT, async {
                loop {
                    if let ClientFrame::Answer { id, state } = router.next().await {
                        return (id, state);
                    }
                }
            })
            .await
            .unwrap_or_else(|_| panic!("no answer within {PROMPT:?} while local clients stalled"));
            assert_eq!(answer, (QueryId(1), HostState::NotSharing {}));
            assert!(asked.elapsed() < PROMPT);

            for task in flood {
                task.abort();
            }
            router.task.abort();
        }

        fn resolve_request(join: bool) -> Request {
            Request::Resolve {
                repo: RepoSlug::try_from("example-org/example-repo").unwrap(),
                ticket: Some(TicketId::new("ENG", 7).unwrap()),
                join,
            }
        }

        fn example_repo_map() -> ServerFrame {
            ServerFrame::Projects {
                projects: vec![sealmesh_proto::ProjectEntry {
                    id: ProjectId::try_from("example-repo").unwrap(),
                    repos: vec![RepoSlug::try_from("example-org/example-repo").unwrap()],
                }],
            }
        }

        fn repo_room() -> RoomId {
            RoomId::new(
                ProjectId::try_from("example-repo").unwrap(),
                Scope::Repo(RepoSlug::try_from("example-org/example-repo").unwrap()),
            )
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_map_request_the_router_never_answers_never_delays_a_query() {
            let timing = Timing {
                router_wait: STALL,
                ..Timing::default()
            };
            let (rooms, local) = rooms("pq", timing, Arc::new(AtomicBool::new(true)));
            let (socket, ws) = connection().await;
            let mut router = FakeRouter::spawn(ws);
            let daemon = daemon(rooms, config(&local.root));
            daemon.send(socket).unwrap();

            // The map request goes out, and this router never answers it.
            assert_eq!(router.next_any().await, ClientFrame::Projects {});
            // A resolution waits on it.
            let path = local.path.clone();
            let _resolving = tokio::spawn(async move {
                connect::request(&path, &resolve_request(false), STALL).await
            });
            wait_for(&local.stats.awaiting_router, 1).await;

            let asked = std::time::Instant::now();
            router.send(ServerFrame::Query {
                id: QueryId(9),
                asked_by: host("shelter"),
            });
            let answer = tokio::time::timeout(PROMPT, async {
                loop {
                    if let ClientFrame::Answer { id, state } = router.next().await {
                        return (id, state);
                    }
                }
            })
            .await
            .unwrap_or_else(|_| panic!("no answer within {PROMPT:?} while the map was awaited"));
            assert_eq!(answer, (QueryId(9), HostState::NotSharing {}));
            assert!(asked.elapsed() < PROMPT);
            assert!(
                local.stats.awaiting_router.load(Ordering::SeqCst) >= 1,
                "the resolution stopped waiting, so this proved nothing"
            );
            router.task.abort();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn an_older_router_that_never_sends_the_map_resolves_to_no_rooms_in_bounded_time() {
            let timing = Timing {
                router_wait: Duration::from_millis(300),
                ..Timing::default()
            };
            let (rooms, local) = rooms("po", timing, Arc::new(AtomicBool::new(true)));
            let (socket, ws) = connection().await;
            let mut router = FakeRouter::spawn(ws);
            let daemon = daemon(rooms, config(&local.root));
            daemon.send(socket).unwrap();
            assert_eq!(router.next_any().await, ClientFrame::Projects {});

            let started = std::time::Instant::now();
            // The session checks deadlines on its one-second tick.
            let answer =
                connect::request(&local.path, &resolve_request(true), Duration::from_secs(10))
                    .await
                    .unwrap();
            assert_eq!(answer, Response::Resolved { rooms: Vec::new() });
            assert!(
                started.elapsed() < Duration::from_secs(4),
                "waited {:?}",
                started.elapsed()
            );
            // Nothing was joined, and nothing asked the router to.
            assert!(
                tokio::time::timeout(Duration::from_millis(200), router.next())
                    .await
                    .is_err()
            );
            router.task.abort();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn resolving_with_join_joins_each_room_through_the_router() {
            let (rooms, local) = rooms("pj", Timing::default(), Arc::new(AtomicBool::new(true)));
            let (socket, ws) = connection().await;
            let mut router = FakeRouter::spawn(ws);
            let daemon = daemon(rooms, config(&local.root));
            daemon.send(socket).unwrap();
            assert_eq!(router.next_any().await, ClientFrame::Projects {});
            router.send(example_repo_map());

            let path = local.path.clone();
            let resolving = tokio::spawn(async move {
                connect::request(&path, &resolve_request(true), Duration::from_secs(20)).await
            });
            // Each room is asked of the router, which decides it: the ticket
            // room is admitted, the repo room is not. No project room is
            // named, since `[chat] project_rooms` is off.
            assert_eq!(router.next().await, ClientFrame::Join { room: ticket(7) });
            router.send(ServerFrame::Joined { room: ticket(7) });
            assert_eq!(router.next().await, ClientFrame::Join { room: repo_room() });
            router.send(ServerFrame::JoinRefused { room: repo_room() });

            assert_eq!(
                resolving.await.unwrap().unwrap(),
                Response::Resolved {
                    rooms: vec![
                        ResolvedRoom {
                            room: ticket(7),
                            join: Some(JoinOutcome::Joined),
                        },
                        ResolvedRoom {
                            room: repo_room(),
                            join: Some(JoinOutcome::Refused),
                        },
                    ],
                }
            );
            let Response::Status { status } = ask(&local, Request::Status {}).await else {
                panic!()
            };
            assert_eq!(status.rooms.len(), 1);
            assert_eq!(status.rooms[0].room, ticket(7));

            // Without `join`, nothing is asked of the router.
            assert_eq!(
                ask(&local, resolve_request(false)).await,
                Response::Resolved {
                    rooms: vec![
                        ResolvedRoom {
                            room: ticket(7),
                            join: None,
                        },
                        ResolvedRoom {
                            room: repo_room(),
                            join: None,
                        },
                    ],
                }
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(200), router.next())
                    .await
                    .is_err()
            );
            router.task.abort();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_reconnect_rejoins_every_room_before_anything_else() {
            let (rooms, local) = rooms("r", Timing::default(), Arc::new(AtomicBool::new(true)));
            let daemon = daemon(rooms, config(&local.root));

            let (socket, ws) = connection().await;
            let mut first = FakeRouter::spawn(ws);
            daemon.send(socket).unwrap();
            join(&local, &mut first, ticket(1)).await;
            join(&local, &mut first, ticket(2)).await;

            // The router goes away.
            drop(first.out);
            first.task.await.unwrap();

            let (socket, ws) = connection().await;
            let mut second = FakeRouter::spawn(ws);
            daemon.send(socket).unwrap();
            assert_eq!(
                second.next_any().await,
                ClientFrame::Join { room: ticket(1) }
            );
            assert_eq!(second.next_any().await, ClientFrame::Projects {});
            second.send(ServerFrame::Joined { room: ticket(1) });
            assert_eq!(second.next().await, ClientFrame::Join { room: ticket(2) });
            second.send(ServerFrame::Joined { room: ticket(2) });

            // Both held again, with no local client having asked.
            let started = std::time::Instant::now();
            loop {
                let Response::Status { status } = ask(&local, Request::Status {}).await else {
                    panic!()
                };
                if status.rooms.iter().all(|r| r.held) && status.connected {
                    assert_eq!(status.rooms.len(), 2);
                    break;
                }
                assert!(started.elapsed() < Duration::from_secs(5), "{status:?}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            second.task.abort();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_refusal_from_a_newer_router_reaches_the_poster_as_unknown() {
            let (rooms, local) = rooms("k", Timing::default(), Arc::new(AtomicBool::new(true)));
            let (socket, ws) = connection().await;
            let mut router = FakeRouter::spawn(ws);
            let daemon = daemon(rooms, config(&local.root));
            daemon.send(socket).unwrap();
            join(&local, &mut router, ticket(1)).await;

            let path = local.path.clone();
            let posting = tokio::spawn(async move {
                connect::request(
                    &path,
                    &Request::Post {
                        room: ticket(1),
                        kind: MessageKind::Claim,
                        body: Body::new("x").unwrap(),
                        consumer: None,
                    },
                    Duration::from_secs(60),
                )
                .await
            });
            assert!(matches!(router.next().await, ClientFrame::Post { .. }));
            router.send_raw(format!(
                r#"{{"type":"post_refused","room":{},"category":"a_category_from_the_future"}}"#,
                serde_json::to_string(&ticket(1)).unwrap()
            ));
            let started = std::time::Instant::now();
            assert_eq!(
                posting.await.unwrap().unwrap(),
                Response::PostRefused {
                    room: ticket(1),
                    category: crate::room::local::Category::UNKNOWN,
                }
            );
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "answered by the deadline, not the refusal"
            );
            let Response::Status { status } = ask(&local, Request::Status {}).await else {
                panic!()
            };
            assert_eq!(status.unknown_refusals, 1);
            router.task.abort();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn with_chat_off_the_socket_says_so_and_no_room_frame_is_sent() {
            let on = Arc::new(AtomicBool::new(false));
            let (rooms, local) = rooms("o", Timing::default(), on);
            let (socket, ws) = connection().await;
            let mut router = FakeRouter::spawn(ws);
            let daemon = daemon(rooms, config(&local.root));
            daemon.send(socket).unwrap();

            for request in [
                Request::Join {
                    room: ticket(1),
                    holder: None,
                },
                Request::ListRooms {},
                Request::Status {},
            ] {
                let started = std::time::Instant::now();
                assert_eq!(ask(&local, request).await, Response::Disabled {});
                assert!(started.elapsed() < Duration::from_secs(1));
            }

            // The first thing the router hears is the answer to its query:
            // nothing about rooms went before it, not even a map request.
            router.send(ServerFrame::Query {
                id: QueryId(4),
                asked_by: host("shelter"),
            });
            assert!(matches!(
                router.next_any().await,
                ClientFrame::Answer { id: QueryId(4), .. }
            ));
            router.task.abort();
        }
    }
}
