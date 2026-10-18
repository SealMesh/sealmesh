//! How long two hosts may talk to each other alone in a room.
//!
//! The token budget in `room_budget` bounds what a loop spends. This bounds
//! the shape that spends it: two agents answering each other with nobody else
//! in the conversation. Such an exchange ends on its own, by construction,
//! rather than when someone notices the bill.
//!
//! # Who counts as an agent
//!
//! The host the poster's credential resolved to at its handshake, as
//! everywhere else in the router (rule 12). The wire has no trusted agent id,
//! so two agents on one laptop are one host posting repeatedly. This cap cannot
//! see that loop. The per-host post rate and token allowance bound it instead.
//!
//! # What is counted
//!
//! Each room has at most one *run*: the admitted posts since a third host last
//! posted there. A run starts with one host, and becomes an exchange when a
//! second host posts. It counts *turns*, not posts. A turn is one or more
//! consecutive admitted posts by one host, so the count goes up only when the
//! poster changes. An exchange's count starts at two: the second host's first
//! post, and the turn of the first host's that it answered.
//!
//! A host posting several times in a row stays on one turn, before anyone
//! answers or after. So one member's status updates cannot run down the cap of
//! a host that posted once, and a run of one host is never capped. How often a
//! single host posts is the post rate's and the token allowance's to bound.
//!
//! When the count reaches [`TurnCap`], the next post by either host in that
//! room is refused, whether or not it would start a new turn, and so is every
//! post by either of them after it. Refusing only the next alternating post
//! would let the other side keep posting into a conversation nobody is
//! answering, and every one of those posts is still input the refused side
//! reads and pays for. So once the cap is reached, the pair is muted in that
//! room, and only there. "Stops within the cap" means exactly this: between two
//! posts by any third host, the two hosts of an exchange take at most the cap's
//! number of turns in that room. A strict ping-pong, one post a turn, gets at
//! most the cap's number of posts.
//!
//! # What resets a run
//!
//! Only an admitted post by a third host. It starts a new run between that host
//! and whoever posted last, counted at two turns like any exchange's start. The
//! pair it interrupted are no longer capped: whichever of them posts next is a
//! third host to the new run. A third host that keeps answering can reset a
//! pair as often as it posts, and that is intended. A conversation with three
//! people in it is not a closed loop.
//!
//! Nothing either participant sends resets a run. There is no reset frame, and
//! a leave, a rejoin, a reconnect, a revoked membership and a room re-forming
//! all leave the run where it was. The run is kept here, keyed by
//! the room, not on the room or the connection, which all of those replace.
//!
//! A post refused by any limit does not count, and does not reset.
//!
//! "A human reading resets the count" is deferred. There is no read signal
//! yet. When one lands, only a read the router observes, by a host that is
//! neither participant, may reset a run, so the pair cannot reset their own
//! loop by reading.
//!
//! # Forgetting a run
//!
//! A run is forgotten once [`QUIET`] has passed with neither participant
//! posting to its room, or trying to: an attempt refused by this cap keeps it
//! alive. A pair that keeps retrying stays muted. A pair that has been silent
//! in that room for an hour has stopped looping, and by then its token
//! allowance there has refilled too, so the cap stops being the one limit
//! still holding them. Silence is not a frame, so no client can send it.
//!
//! This is also what bounds the table. A run is created only by an admitted
//! post. The post rate is keyed by the host, so a reconnect does not reset it,
//! and it admits at most `room_budget::POST_BURST` posts plus one every
//! `room_budget::POST_REFILL`: about 1,800 an hour. Each new room also needs a
//! join, at most `rooms::MAX_JOINS_PER_WINDOW` a minute. So one host can
//! create at most about 1,800 runs an hour, and each is forgotten an hour after
//! it goes quiet. A refused attempt only extends a run that already exists, in
//! one of the at most `rooms::MAX_ROOMS_PER_CONNECTION` rooms its connection
//! holds. The table is therefore bounded by the org's membership times that
//! hourly figure.
//!
//! Quiet runs are swept when a new room's run finds the table at [`SWEEP_AT`].
//! After that, a sweep runs only once the table has doubled since the last one.
//! The scan runs under the registry lock, and without that rule a table full
//! of live runs would be rescanned for every new room. With it, the scan costs
//! amortised constant time per new room, and the table holds at most twice
//! the runs that survived the last sweep, or [`SWEEP_AT`].
//!
//! The token allowance's own table does not bound this one. An allowance that
//! spent a single short post refills in seconds, so its per-host room cap
//! limits how many rooms a host holds at once, not how many it churns through.
//!
//! # In-process
//!
//! Like the rooms themselves. A deploy forgets every run, and also empties
//! every room, so a loop has to start again from nothing.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use sealmesh_chat_wire::RoomId;
use sealmesh_wire::HostId;

/// The environment variable that sets the cap.
pub const TURN_CAP_VAR: &str = "SEALMESH_ROOM_TURN_CAP";

/// How long a run is kept with neither participant posting to its room. The
/// token allowance's own refill horizon, for the reason in the module docs.
pub const QUIET: Duration = Duration::from_secs(60 * 60);

/// How many rooms' runs are kept before quiet ones are first swept out. Later
/// sweeps wait for the table to double, as the module docs describe.
pub const SWEEP_AT: usize = 1024;

/// Turns two hosts may take in an exchange in one room before a third host
/// posts.
///
/// The only constructor refuses a value outside
/// [`MIN`](Self::MIN)..=[`MAX`](Self::MAX), so no configuration can raise the
/// cap past the ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurnCap(u32);

impl TurnCap {
    /// Ten turns each. A real coordination exchange between two agents is a
    /// claim, a question, an answer and an ack, and a conflict worth
    /// negotiating is perhaps a dozen turns. Twenty covers that with room to
    /// spare. At the post rate, two agents answering at once reach it in
    /// about six seconds, and it is well inside the token allowance: at one
    /// 1,000-character post a turn, the exchange costs each host 3,500 of its
    /// 20,000.
    pub const DEFAULT: Self = Self(20);

    /// Five times the default. At the default allowance, a ping-pong of
    /// 1,000-character posts is refused by the budget after 57 posts from each
    /// host, 114 turns together. A cap above 100 would let that loop run into
    /// the budget first, and the cap would stop bounding the shape.
    pub const MAX: Self = Self(100);

    /// A claim, a question, an answer and an ack. Anything lower would refuse
    /// the shortest useful exchange. It must stay at least 2, the count an
    /// exchange starts at.
    pub const MIN: Self = Self(4);

    /// No cap worth the name. Test builds only, for tests about delivery
    /// rather than limits; nothing a configuration reaches can build it.
    #[cfg(test)]
    pub(crate) const UNCAPPED: Self = Self(u32::MAX);

    pub fn new(turns: u64) -> Result<Self, TurnCapError> {
        if turns < u64::from(Self::MIN.0) {
            return Err(TurnCapError::BelowFloor { turns });
        }
        if turns > u64::from(Self::MAX.0) {
            return Err(TurnCapError::AboveCeiling { turns });
        }
        u32::try_from(turns)
            .map(Self)
            .map_err(|_| TurnCapError::AboveCeiling { turns })
    }

    pub fn get(self) -> u32 {
        self.0
    }
}

impl Default for TurnCap {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TurnCapError {
    #[error("{TURN_CAP_VAR} is not valid UTF-8")]
    NotUnicode,
    #[error("{TURN_CAP_VAR} is not a whole number of turns: {raw:?}")]
    NotANumber { raw: String },
    #[error(
        "{TURN_CAP_VAR}={turns} is below the floor of {}, which would refuse the shortest useful exchange",
        TurnCap::MIN.0
    )]
    BelowFloor { turns: u64 },
    #[error("{TURN_CAP_VAR}={turns} exceeds the hard ceiling of {}", TurnCap::MAX.0)]
    AboveCeiling { turns: u64 },
}

/// The configured cap, or the default when the variable is unset.
///
/// A malformed or out-of-range value is refused, so the router does not start.
/// It is never clamped, the same as `SEALMESH_ROOM_TOKENS_PER_HOUR`. The
/// value is the operator's own, so quoting it back is safe (rule 14 is about
/// peers).
pub fn parse_turn_cap(raw: Option<&str>) -> Result<TurnCap, TurnCapError> {
    let Some(raw) = raw else {
        return Ok(TurnCap::DEFAULT);
    };
    let turns: u64 = raw.trim().parse().map_err(|_| TurnCapError::NotANumber {
        raw: raw.to_owned(),
    })?;
    TurnCap::new(turns)
}

/// [`parse_turn_cap`] for the variable as `std::env::var` returned it. Only an
/// unset variable means the default.
pub fn turn_cap_from_env(
    value: Result<String, std::env::VarError>,
) -> Result<TurnCap, TurnCapError> {
    match value {
        Ok(raw) => parse_turn_cap(Some(&raw)),
        Err(std::env::VarError::NotPresent) => Ok(TurnCap::DEFAULT),
        Err(std::env::VarError::NotUnicode(_)) => Err(TurnCapError::NotUnicode),
    }
}

/// What [`TurnRuns::check`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Turn {
    /// This cap does not refuse the post.
    Open,
    /// The poster is in an exchange that has reached the cap. `notify` names
    /// the other participant on the first refusal of this run, and is `None`
    /// on every refusal after it, so the other side is told once and a
    /// retrying poster cannot flood it.
    Capped { notify: Option<HostId> },
}

#[derive(Debug)]
struct Run {
    first: HostId,
    second: Option<HostId>,
    /// Who made the run's last admitted post. A post by the other participant
    /// is a new turn; another post by this host is the same one.
    last: HostId,
    /// Turns taken in the run: one while only `first` has posted, then one
    /// more each time the poster changes.
    turns: u32,
    /// The last admitted post, or capped attempt, by a participant.
    touched: Instant,
    /// Whether the other participant has been told this run is capped.
    notified: bool,
}

impl Run {
    fn new(host: HostId, now: Instant) -> Self {
        Self {
            first: host.clone(),
            second: None,
            last: host,
            turns: 1,
            touched: now,
            notified: false,
        }
    }

    /// The run a third host's post starts: that host answering the last post
    /// before it, so two turns, like any exchange's start.
    fn answering(last: HostId, host: HostId, now: Instant) -> Self {
        Self {
            first: last,
            second: Some(host.clone()),
            last: host,
            turns: 2,
            touched: now,
            notified: false,
        }
    }

    fn is_quiet(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.touched) >= QUIET
    }
}

/// Where a post lands relative to a room's run.
enum Place {
    First,
    Second,
    NewSecond,
    Third,
}

/// Every room's current run.
#[derive(Debug)]
pub struct TurnRuns {
    cap: TurnCap,
    rooms: HashMap<RoomId, Run>,
    /// The table size at which the next new room sweeps quiet runs out.
    next_sweep: usize,
}

impl Default for TurnRuns {
    fn default() -> Self {
        Self::new(TurnCap::DEFAULT)
    }
}

impl TurnRuns {
    pub fn new(cap: TurnCap) -> Self {
        Self {
            cap,
            rooms: HashMap::new(),
            next_sweep: SWEEP_AT,
        }
    }

    /// Whether `host`'s post to `room` is refused by the cap. Changes nothing
    /// unless it refuses, so the caller can run its other limits and admit or
    /// refuse after it. A refusal counts as activity in the run.
    pub fn check(&mut self, room: &RoomId, host: &HostId, now: Instant) -> Turn {
        let cap = self.cap.get();
        let Some(run) = self.rooms.get_mut(room) else {
            return Turn::Open;
        };
        if run.is_quiet(now) || run.turns < cap {
            return Turn::Open;
        }
        let other = match (&run.second, Self::place(run, host)) {
            (Some(second), Place::First) => second.clone(),
            (_, Place::Second) => run.first.clone(),
            _ => return Turn::Open,
        };
        run.touched = now;
        let first_refusal = !run.notified;
        run.notified = true;
        Turn::Capped {
            notify: first_refusal.then_some(other),
        }
    }

    /// Counts an admitted post by `host` to `room`. Call only once the post has
    /// passed every limit and is about to be delivered.
    pub fn record(&mut self, room: &RoomId, host: &HostId, now: Instant) {
        if !self.rooms.contains_key(room) && self.rooms.len() >= self.next_sweep {
            self.rooms.retain(|_, run| !run.is_quiet(now));
            self.next_sweep = self.rooms.len().saturating_mul(2).max(SWEEP_AT);
        }
        let Some(run) = self.rooms.get_mut(room) else {
            self.rooms.insert(room.clone(), Run::new(host.clone(), now));
            return;
        };
        if run.is_quiet(now) {
            *run = Run::new(host.clone(), now);
            return;
        }
        match Self::place(run, host) {
            Place::First | Place::Second => {
                // Another post by the host that posted last is the same turn.
                if run.last != *host {
                    run.turns = run.turns.saturating_add(1);
                    run.last = host.clone();
                }
                run.touched = now;
            }
            Place::NewSecond => {
                run.second = Some(host.clone());
                run.last = host.clone();
                run.turns = run.turns.saturating_add(1);
                run.touched = now;
            }
            Place::Third => *run = Run::answering(run.last.clone(), host.clone(), now),
        }
    }

    fn place(run: &Run, host: &HostId) -> Place {
        if *host == run.first {
            return Place::First;
        }
        match &run.second {
            Some(second) if second == host => Place::Second,
            Some(_) => Place::Third,
            None => Place::NewSecond,
        }
    }

    #[cfg(test)]
    pub(crate) fn tracked(&self) -> usize {
        self.rooms.len()
    }

    #[cfg(test)]
    pub(crate) fn next_sweep(&self) -> usize {
        self.next_sweep
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

    use sealmesh_chat_wire::Scope;
    use sealmesh_wire::{ProjectId, TicketId};

    use super::*;

    fn host(name: &str) -> HostId {
        HostId::try_from(name).unwrap()
    }

    fn ticket(number: u32) -> RoomId {
        RoomId::new(
            ProjectId::try_from("example-repo").unwrap(),
            Scope::Ticket(TicketId::new("ENG", number).unwrap()),
        )
    }

    /// Alternates `a` and `b` in `room` until the cap refuses one, recording
    /// each admitted post. Returns how many were admitted.
    fn exchange(runs: &mut TurnRuns, room: &RoomId, a: &HostId, b: &HostId, now: Instant) -> u32 {
        let mut admitted = 0;
        for i in 0.. {
            let poster = if i % 2 == 0 { a } else { b };
            if runs.check(room, poster, now) != Turn::Open {
                return admitted;
            }
            runs.record(room, poster, now);
            admitted += 1;
            assert!(admitted <= TurnCap::MAX.get(), "the cap never refused");
        }
        unreachable!()
    }

    #[test]
    fn an_unset_cap_is_the_default_and_one_inside_the_bounds_is_used() {
        assert_eq!(parse_turn_cap(None), Ok(TurnCap::DEFAULT));
        assert_eq!(parse_turn_cap(Some(" 30 ")).unwrap().get(), 30);
        assert_eq!(parse_turn_cap(Some("100")), Ok(TurnCap::MAX));
        assert_eq!(parse_turn_cap(Some("4")), Ok(TurnCap::MIN));
    }

    #[test]
    fn no_configuration_can_raise_the_cap_past_the_ceiling() {
        for over in [
            u64::from(TurnCap::MAX.get()) + 1,
            u64::from(u32::MAX) + 1,
            u64::MAX,
        ] {
            assert_eq!(
                parse_turn_cap(Some(&over.to_string())),
                Err(TurnCapError::AboveCeiling { turns: over })
            );
            assert_eq!(
                TurnCap::new(over),
                Err(TurnCapError::AboveCeiling { turns: over })
            );
        }
        assert!(matches!(
            parse_turn_cap(Some("18446744073709551616")),
            Err(TurnCapError::NotANumber { .. })
        ));
    }

    #[test]
    fn a_cap_below_the_shortest_useful_exchange_is_refused() {
        for under in [0, 1, u64::from(TurnCap::MIN.get()) - 1] {
            assert_eq!(
                parse_turn_cap(Some(&under.to_string())),
                Err(TurnCapError::BelowFloor { turns: under })
            );
        }
        assert!(TurnCap::MIN.get() >= 2, "an exchange starts at two");
    }

    #[test]
    fn a_malformed_or_non_utf8_cap_is_an_error_not_a_default() {
        use std::{env::VarError, ffi::OsString};

        for raw in ["", "lots", "-1", "20.0", "1e2"] {
            let error = parse_turn_cap(Some(raw)).unwrap_err();
            assert!(error.to_string().contains(TURN_CAP_VAR), "{raw:?}: {error}");
        }
        assert_eq!(
            turn_cap_from_env(Err(VarError::NotPresent)),
            Ok(TurnCap::DEFAULT)
        );
        assert_eq!(
            turn_cap_from_env(Err(VarError::NotUnicode(OsString::from("x")))),
            Err(TurnCapError::NotUnicode)
        );
        assert_eq!(
            turn_cap_from_env(Ok("12".to_owned())).map(TurnCap::get),
            Ok(12)
        );
    }

    #[test]
    fn an_exchange_is_admitted_up_to_the_cap_and_then_both_sides_are_refused() {
        let now = Instant::now();
        let mut runs = TurnRuns::new(TurnCap::MIN);
        let (atlas, bay) = (host("atlas"), host("bay"));
        assert_eq!(
            exchange(&mut runs, &ticket(1), &atlas, &bay, now),
            TurnCap::MIN.get()
        );

        // The post that hit the cap was atlas's; bay is told, once.
        assert_eq!(
            runs.check(&ticket(1), &atlas, now),
            Turn::Capped { notify: None },
            "the refusal that hit the cap was already reported by exchange()"
        );
        assert_eq!(
            runs.check(&ticket(1), &bay, now),
            Turn::Capped { notify: None }
        );
    }

    #[test]
    fn the_first_refusal_names_the_other_participant_and_no_later_one_does() {
        let now = Instant::now();
        let mut runs = TurnRuns::new(TurnCap::MIN);
        let (atlas, bay) = (host("atlas"), host("bay"));
        for i in 0..TurnCap::MIN.get() {
            runs.record(&ticket(1), if i % 2 == 0 { &atlas } else { &bay }, now);
        }
        assert_eq!(
            runs.check(&ticket(1), &bay, now),
            Turn::Capped {
                notify: Some(atlas.clone())
            }
        );
        for _ in 0..8 {
            assert_eq!(
                runs.check(&ticket(1), &atlas, now),
                Turn::Capped { notify: None }
            );
            assert_eq!(
                runs.check(&ticket(1), &bay, now),
                Turn::Capped { notify: None }
            );
        }
    }

    #[test]
    fn a_host_posting_alone_is_never_capped_and_its_monologue_does_not_count() {
        let now = Instant::now();
        let mut runs = TurnRuns::new(TurnCap::MIN);
        let (atlas, bay) = (host("atlas"), host("bay"));
        for _ in 0..TurnCap::MAX.get() * 2 {
            assert_eq!(runs.check(&ticket(1), &atlas, now), Turn::Open);
            runs.record(&ticket(1), &atlas, now);
        }
        // bay answers: the exchange is his turn and the one it answered. From
        // there only a change of poster counts.
        runs.record(&ticket(1), &bay, now);
        for turn in 2..TurnCap::MIN.get() {
            let poster = if turn % 2 == 0 { &atlas } else { &bay };
            assert_eq!(runs.check(&ticket(1), poster, now), Turn::Open);
            runs.record(&ticket(1), poster, now);
        }
        assert!(matches!(
            runs.check(&ticket(1), &atlas, now),
            Turn::Capped { .. }
        ));
    }

    #[test]
    fn a_third_host_resets_the_run_and_its_refused_attempt_does_not() {
        let now = Instant::now();
        let mut runs = TurnRuns::new(TurnCap::MIN);
        let (atlas, bay, cedar) = (host("atlas"), host("bay"), host("cedar"));
        exchange(&mut runs, &ticket(1), &atlas, &bay, now);

        // cedar is not refused by the cap, and checking does not reset.
        assert_eq!(runs.check(&ticket(1), &cedar, now), Turn::Open);
        assert!(matches!(
            runs.check(&ticket(1), &atlas, now),
            Turn::Capped { .. }
        ));

        // Her admitted post does.
        runs.record(&ticket(1), &cedar, now);
        assert_eq!(runs.check(&ticket(1), &atlas, now), Turn::Open);
        assert_eq!(runs.check(&ticket(1), &bay, now), Turn::Open);
        // And the two of them are capped again after the full cap, no more:
        // atlas's reply to cedar is the turn bay's post then answers.
        assert_eq!(
            exchange(&mut runs, &ticket(1), &atlas, &bay, now),
            TurnCap::MIN.get(),
            "a third host's post did not start a fresh exchange of exactly the cap"
        );
    }

    #[test]
    fn a_monologue_after_one_post_never_caps_the_silent_host() {
        // The review finding on PR #98: counting posts rather than turns let
        // one member mute a host that had posted once, by posting cap times.
        for cap in [TurnCap::MIN, TurnCap::DEFAULT] {
            let now = Instant::now();
            let mut runs = TurnRuns::new(cap);
            let (cedar, atlas) = (host("cedar"), host("atlas"));
            runs.record(&ticket(1), &cedar, now);
            for i in 0..TurnCap::MAX.get() * 3 {
                assert_eq!(
                    runs.check(&ticket(1), &atlas, now),
                    Turn::Open,
                    "{cap:?}: atlas's status update {i} was refused"
                );
                runs.record(&ticket(1), &atlas, now);
            }
            assert_eq!(
                runs.check(&ticket(1), &cedar, now),
                Turn::Open,
                "{cap:?}: cedar was muted by atlas's monologue"
            );
        }
    }

    #[test]
    fn a_pair_gets_at_most_the_cap_in_turns_per_third_host_post() {
        for cap in [TurnCap::MIN, TurnCap::DEFAULT] {
            let now = Instant::now();
            let mut runs = TurnRuns::new(cap);
            let (atlas, bay, cedar) = (host("atlas"), host("bay"), host("cedar"));
            for round in 0..4 {
                if round > 0 {
                    runs.record(&ticket(1), &cedar, now);
                }
                let (a, b) = if round % 2 == 0 {
                    (&atlas, &bay)
                } else {
                    (&bay, &atlas)
                };
                assert_eq!(
                    exchange(&mut runs, &ticket(1), a, b, now),
                    cap.get(),
                    "{cap:?}, round {round}: a strict ping-pong got more than the cap"
                );
            }
        }
    }

    #[test]
    fn repeat_posts_within_a_turn_do_not_count_and_a_capped_pair_stays_muted() {
        let now = Instant::now();
        let mut runs = TurnRuns::new(TurnCap::MIN);
        let (atlas, bay) = (host("atlas"), host("bay"));
        // Every turn but the last is three posts in a row; the second and third
        // posts of a turn count nothing.
        for turn in 0..TurnCap::MIN.get() - 1 {
            let poster = if turn % 2 == 0 { &atlas } else { &bay };
            for _ in 0..3 {
                assert_eq!(
                    runs.check(&ticket(1), poster, now),
                    Turn::Open,
                    "turn {turn}"
                );
                runs.record(&ticket(1), poster, now);
            }
        }
        // bay's post takes the last turn the cap allows. After it, both are
        // refused: atlas's next turn, and another post of bay's own. The pair
        // is muted, not just the turn.
        assert_eq!(runs.check(&ticket(1), &bay, now), Turn::Open);
        runs.record(&ticket(1), &bay, now);
        assert!(matches!(
            runs.check(&ticket(1), &atlas, now),
            Turn::Capped { .. }
        ));
        assert!(matches!(
            runs.check(&ticket(1), &bay, now),
            Turn::Capped { .. }
        ));
    }

    #[test]
    fn a_run_is_per_room() {
        let now = Instant::now();
        let mut runs = TurnRuns::new(TurnCap::MIN);
        let (atlas, bay) = (host("atlas"), host("bay"));
        exchange(&mut runs, &ticket(1), &atlas, &bay, now);
        assert_eq!(runs.check(&ticket(2), &atlas, now), Turn::Open);
        assert_eq!(
            exchange(&mut runs, &ticket(2), &atlas, &bay, now),
            TurnCap::MIN.get()
        );
    }

    #[test]
    fn a_capped_pair_that_keeps_trying_stays_capped_and_one_that_goes_quiet_is_released() {
        let start = Instant::now();
        let mut runs = TurnRuns::new(TurnCap::MIN);
        let (atlas, bay) = (host("atlas"), host("bay"));
        exchange(&mut runs, &ticket(1), &atlas, &bay, start);

        // Retrying every half hour, for three hours: never released.
        let half = QUIET / 2;
        let mut at = start;
        for _ in 0..6 {
            at += half;
            assert!(
                matches!(runs.check(&ticket(1), &atlas, at), Turn::Capped { .. }),
                "a retrying pair was released"
            );
        }
        // One tick short of a silent hour: still capped, and that attempt
        // restarts the hour.
        assert!(matches!(
            runs.check(&ticket(1), &bay, at + QUIET - Duration::from_nanos(1)),
            Turn::Capped { .. }
        ));
        let silent = at + QUIET - Duration::from_nanos(1) + QUIET;
        assert_eq!(runs.check(&ticket(1), &atlas, silent), Turn::Open);
        runs.record(&ticket(1), &atlas, silent);
        assert_eq!(
            exchange(&mut runs, &ticket(1), &bay, &atlas, silent),
            TurnCap::MIN.get() - 1
        );
    }

    #[test]
    fn a_clock_that_reads_earlier_neither_releases_nor_panics() {
        let start = Instant::now() + QUIET;
        let mut runs = TurnRuns::new(TurnCap::MIN);
        let (atlas, bay) = (host("atlas"), host("bay"));
        exchange(&mut runs, &ticket(1), &atlas, &bay, start);
        assert!(matches!(
            runs.check(&ticket(1), &atlas, start - QUIET),
            Turn::Capped { .. }
        ));
    }

    #[test]
    fn an_uncapped_count_saturates_rather_than_wrapping() {
        let now = Instant::now();
        let mut runs = TurnRuns::new(TurnCap::UNCAPPED);
        let (atlas, bay) = (host("atlas"), host("bay"));
        runs.record(&ticket(1), &atlas, now);
        runs.record(&ticket(1), &bay, now);
        runs.rooms.get_mut(&ticket(1)).unwrap().turns = u32::MAX - 1;
        for i in 0..4 {
            runs.record(&ticket(1), if i % 2 == 0 { &atlas } else { &bay }, now);
        }
        assert_eq!(runs.rooms[&ticket(1)].turns, u32::MAX);
        assert!(matches!(
            runs.check(&ticket(1), &bay, now),
            Turn::Capped { .. }
        ));
    }

    #[test]
    fn quiet_runs_are_swept_once_the_table_is_full_and_live_ones_are_kept() {
        let start = Instant::now();
        let mut runs = TurnRuns::new(TurnCap::MIN);
        let (atlas, bay) = (host("atlas"), host("bay"));
        exchange(&mut runs, &ticket(9999), &atlas, &bay, start);
        let full = u32::try_from(SWEEP_AT).unwrap();
        for n in 1..full {
            runs.record(&ticket(n), &atlas, start);
        }
        assert_eq!(runs.tracked(), SWEEP_AT);

        // Keep the capped run alive, then let everything else go quiet.
        let later = start + QUIET;
        assert!(matches!(
            runs.check(&ticket(9999), &bay, start + QUIET / 2),
            Turn::Capped { .. }
        ));
        runs.record(&ticket(full), &atlas, later);
        assert_eq!(
            runs.tracked(),
            2,
            "the sweep kept a quiet run or lost a live one"
        );
        assert!(matches!(
            runs.check(&ticket(9999), &atlas, later),
            Turn::Capped { .. }
        ));
    }

    #[test]
    fn a_table_full_of_live_runs_is_swept_only_once_it_has_doubled() {
        // The review finding on PR #98: past the threshold with every run
        // live, each new room rescanned the whole table under the lock.
        let start = Instant::now();
        let mut runs = TurnRuns::default();
        let atlas = host("atlas");
        let full = u32::try_from(SWEEP_AT).unwrap();
        for n in 1..=full {
            runs.record(&ticket(n), &atlas, start);
        }
        assert_eq!(runs.tracked(), SWEEP_AT);

        // The next new room sweeps once, keeps every live run, and doubles the
        // threshold.
        runs.record(&ticket(full + 1), &atlas, start);
        assert_eq!(runs.tracked(), SWEEP_AT + 1);
        assert_eq!(runs.next_sweep(), SWEEP_AT * 2);

        // Everything so far goes quiet. New rooms below the doubled threshold
        // do not sweep, so the quiet runs are still in the table.
        let later = start + QUIET;
        for n in full + 2..=full * 2 {
            runs.record(&ticket(n), &atlas, later);
        }
        assert_eq!(
            runs.tracked(),
            SWEEP_AT * 2,
            "a sweep ran before the table doubled"
        );

        // At the threshold the next new room sweeps, and collects every quiet
        // run.
        runs.record(&ticket(full * 2 + 1), &atlas, later);
        assert_eq!(runs.tracked(), SWEEP_AT, "the sweep missed a quiet run");
        assert_eq!(runs.next_sweep(), (SWEEP_AT - 1) * 2);
    }
}
