//! What one post may cost, and who pays for it.
//!
//! Two agents that can talk to each other can loop, and a loop costs money on
//! both ends: every message one agent posts is input the others read. Rooms
//! therefore carry the two limits in this module and a turn cap in [`crate::turn_cap`], all enforced by the router at ingest, before a
//! post takes a sequence number or is cloned into anyone's queue. A limit a
//! daemon enforced on itself would not be a limit.
//!
//! # A post rate, per posting host
//!
//! [`POST_BURST`] posts at once, then one every [`POST_REFILL`], across every
//! room a host posts to. This limit is about the room, not the bill. Every
//! member has a [`ROOM_QUEUE`]-slot queue, and without a rate limit one member
//! could post fast enough to keep every other member's queue full, so everyone
//! else's messages would miss them. A burst of eight is a quarter of that
//! queue, so one host's burst cannot fill it. One post every two seconds is
//! thirty a minute, more than any agent coordinating with others needs. It is
//! keyed by the credential-derived host, like the token budget: kept on the
//! connection, it reset on every reconnect, and reconnect-join-post cycles
//! bought a fresh burst each time.
//!
//! # A token budget, per room and per posting host
//!
//! Each posting host has an allowance of [`TokensPerHour`] in each room it
//! posts to. The allowance starts full and refills continuously, so a host
//! that has spent all of it gets back one maximum-size post after about a
//! quarter of an hour at the default. The host is the one the poster's
//! credential resolved to at its handshake, never anything a client sent
//! (rule 12). Keying on the host rather than the connection means a
//! reconnect does not reset the allowance. Keying on the room as well means
//! a host that loops in one room can still post in another, and one chatty
//! host cannot spend anyone else's allowance.
//!
//! # What a token is
//!
//! A conservative estimate from the body, taken before any model sees it:
//! one token per [`BYTES_PER_TOKEN`] bytes of UTF-8, rounded up, plus
//! [`ENVELOPE_TOKENS`] for the room, sender and kind a reader also takes in.
//! Common tokenizers average about four bytes per token on English prose and
//! code, so three over-counts by a third there. CJK text, at three bytes and
//! roughly one token per character, comes out about even. Over-counting is
//! the safe direction: a looping agent is muted a little sooner. Adversarial
//! byte soup can under-count, and the post rate and the frame cap bound that
//! case.
//!
//! # Refusals are visible
//!
//! A refused post is answered with `ServerFrame::PostRefused`, naming the room
//! and whether the post rate or the token budget refused it. An agent that
//! cannot tell it was muted keeps trying.
//!
//! # In-process
//!
//! Every allowance lives in this router process, like the rooms themselves.
//! A deploy resets them all, which is acceptable: a deploy also empties every
//! room, so a loop that was running has to start again from nothing.
//!
//! [`ROOM_QUEUE`]: crate::rooms::ROOM_QUEUE

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::{Duration, Instant},
};

use sealmesh_chat_wire::{Body, MAX_BODY_CHARS, RoomId};
use sealmesh_wire::HostId;

/// Posts one host may make at once before the rate limit applies.
pub const POST_BURST: u64 = 8;

/// How long one host waits to earn back one post.
pub const POST_REFILL: Duration = Duration::from_secs(2);

/// UTF-8 bytes counted as one token. See the module docs.
pub const BYTES_PER_TOKEN: u64 = 3;

/// Tokens charged per post on top of its body, for the room, the sender and
/// the kind every reader also takes in. It also means a stream of one-character
/// posts still costs something.
pub const ENVELOPE_TOKENS: u64 = 16;

/// What the largest post a `Body` can hold is charged: `MAX_BODY_CHARS`
/// characters of four UTF-8 bytes each.
pub const MAX_POST_TOKENS: u64 =
    (MAX_BODY_CHARS as u64 * 4).div_ceil(BYTES_PER_TOKEN) + ENVELOPE_TOKENS;

/// The environment variable that sets each host's allowance per room.
pub const TOKENS_PER_HOUR_VAR: &str = "SEALMESH_ROOM_TOKENS_PER_HOUR";

/// Most room allowances one host may have in use at once.
///
/// An allowance is kept only until it has refilled, at most an hour after
/// that host's last post in that room. Sixty-four is four times the rooms one
/// connection can hold. A host that has posted to sixty-four different rooms
/// within the hour is churning rooms, not coordinating. Its post to a
/// sixty-fifth room is refused until one of its allowances refills. The cap is
/// per host, so no host can use up another host's slots. Together with the
/// org's membership, it bounds this table.
pub const MAX_ALLOWANCES_PER_HOST: usize = 64;

const HOUR: Duration = Duration::from_secs(60 * 60);

/// Tokens one host may post to one room per hour, which is also the most it
/// can post in one burst.
///
/// The only constructor refuses a value outside
/// [`MIN`](Self::MIN)..=[`MAX`](Self::MAX). No configuration can raise the
/// allowance past the ceiling, or set it low enough to mute a room.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokensPerHour(u64);

impl TokensPerHour {
    /// About two hundred short coordination messages an hour, or three
    /// maximum-size ones, from one host to one room. Two agents looping on
    /// 1,000-character replies every ten seconds use it up in about twelve
    /// minutes, then get one such reply a minute.
    pub const DEFAULT: Self = Self(20_000);

    /// Ten times the default. Much above this, a loop would take hours to hit
    /// the limit, and the budget would stop bounding one.
    pub const MAX: Self = Self(200_000);

    /// One maximum-size post. Anything lower would refuse that post forever,
    /// and a mute that can never lift reads as a broken room.
    pub const MIN: Self = Self(MAX_POST_TOKENS);

    /// No limit worth the name. Test builds only, for tests about delivery
    /// rather than limits; nothing a configuration reaches can build it.
    #[cfg(test)]
    pub(crate) const UNMETERED: Self = Self(u64::MAX);

    pub fn new(tokens: u64) -> Result<Self, AllowanceError> {
        if tokens < Self::MIN.0 {
            return Err(AllowanceError::BelowFloor { tokens });
        }
        if tokens > Self::MAX.0 {
            return Err(AllowanceError::AboveCeiling { tokens });
        }
        Ok(Self(tokens))
    }

    pub fn get(self) -> u64 {
        self.0
    }

    /// How long it takes to earn back one token. Never zero, so it can never
    /// be divided by (rule 40). One nanosecond would mean an allowance above
    /// 3.6 trillion an hour, which the ceiling rules out anyway.
    fn per_token(self) -> Duration {
        let nanos = HOUR.as_nanos().checked_div(u128::from(self.0)).unwrap_or(0);
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX).max(1))
    }
}

impl Default for TokensPerHour {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AllowanceError {
    #[error("{TOKENS_PER_HOUR_VAR} is not valid UTF-8")]
    NotUnicode,
    #[error("{TOKENS_PER_HOUR_VAR} is not a whole number of tokens: {raw:?}")]
    NotANumber { raw: String },
    #[error(
        "{TOKENS_PER_HOUR_VAR}={tokens} is below the floor of {MAX_POST_TOKENS}, which would refuse the largest post forever"
    )]
    BelowFloor { tokens: u64 },
    #[error("{TOKENS_PER_HOUR_VAR}={tokens} exceeds the hard ceiling of {}", TokensPerHour::MAX.0)]
    AboveCeiling { tokens: u64 },
}

/// The configured allowance, or the default when the variable is unset.
///
/// A malformed or out-of-range value is refused, so the router does not start,
/// rather than being clamped. An operator who asked for a number gets that
/// number or an error, never a quietly different budget. This follows
/// `SEALMESH_ACCESS_CACHE_TTL_SECS`. The value is the operator's own, so
/// quoting it back in the error is safe (rule 14 is about peers).
pub fn parse_tokens_per_hour(raw: Option<&str>) -> Result<TokensPerHour, AllowanceError> {
    let Some(raw) = raw else {
        return Ok(TokensPerHour::DEFAULT);
    };
    let tokens: u64 = raw.trim().parse().map_err(|_| AllowanceError::NotANumber {
        raw: raw.to_owned(),
    })?;
    TokensPerHour::new(tokens)
}

/// [`parse_tokens_per_hour`] for the variable as `std::env::var` returned it.
///
/// Only an unset variable means the default. A variable set to bytes that are
/// not UTF-8 is refused like any other malformed value, never read as unset.
pub fn tokens_per_hour_from_env(
    value: Result<String, std::env::VarError>,
) -> Result<TokensPerHour, AllowanceError> {
    match value {
        Ok(raw) => parse_tokens_per_hour(Some(&raw)),
        Err(std::env::VarError::NotPresent) => Ok(TokensPerHour::DEFAULT),
        Err(std::env::VarError::NotUnicode(_)) => Err(AllowanceError::NotUnicode),
    }
}

/// What `body` costs to post, in tokens. See the module docs for the estimate.
pub fn tokens(body: &Body) -> u64 {
    u64::try_from(body.as_str().len())
        .unwrap_or(u64::MAX)
        .div_ceil(BYTES_PER_TOKEN)
        .saturating_add(ENVELOPE_TOKENS)
}

/// A refilling allowance: `capacity` to start with, one more every `interval`,
/// never above `capacity`. The same arithmetic as the daemon's `QueryBudget`.
#[derive(Debug, Clone)]
pub struct Bucket {
    tokens: u64,
    capacity: u64,
    interval: Duration,
    filled_to: Instant,
}

impl Bucket {
    pub fn new(capacity: u64, interval: Duration, now: Instant) -> Self {
        Self {
            tokens: capacity,
            capacity,
            interval,
            filled_to: now,
        }
    }

    /// One host's allowance in one room.
    pub fn allowance(per_hour: TokensPerHour, now: Instant) -> Self {
        Self::new(per_hour.get(), per_hour.per_token(), now)
    }

    /// Takes `cost` when the balance covers all of it. A refusal takes nothing,
    /// so a large post that does not fit leaves room for a smaller one.
    pub fn try_spend(&mut self, cost: u64, now: Instant) -> bool {
        self.earn(now);
        match self.tokens.checked_sub(cost) {
            Some(left) => {
                self.tokens = left;
                true
            }
            None => false,
        }
    }

    /// Whether the balance has refilled to capacity by `now`. A full
    /// allowance is the same as a new one, so it can be forgotten.
    pub fn is_full(&self, now: Instant) -> bool {
        self.tokens.saturating_add(self.earned(now).0) >= self.capacity
    }

    /// Tokens earned since `filled_to`, and the time that earning used up.
    /// An interval of zero is treated as instant refill rather than divided by
    /// (rule 40). A clock that reads earlier than `filled_to` earns nothing.
    fn earned(&self, now: Instant) -> (u64, Duration) {
        let interval = self.interval.as_nanos();
        if interval == 0 {
            return (self.capacity, now.saturating_duration_since(self.filled_to));
        }
        let elapsed = now.saturating_duration_since(self.filled_to).as_nanos();
        let earned = elapsed.checked_div(interval).unwrap_or(0);
        let used = earned.saturating_mul(interval);
        (
            u64::try_from(earned).unwrap_or(u64::MAX),
            Duration::from_nanos(u64::try_from(used).unwrap_or(u64::MAX)),
        )
    }

    fn earn(&mut self, now: Instant) {
        let (earned, used) = self.earned(now);
        if earned == 0 {
            return;
        }
        self.tokens = self.tokens.saturating_add(earned).min(self.capacity);
        self.filled_to = self.filled_to.checked_add(used).unwrap_or(now);
    }
}

/// How many hosts' post rates are kept before refilled ones are swept out.
///
/// A host's rate refills to full [`POST_BURST`] × [`POST_REFILL`] (sixteen
/// seconds) after its last post, and a full rate is the same as a fresh one,
/// so it can be forgotten. The sweep runs only when a host not already tracked
/// posts, so the table holds the hosts that posted in the last sixteen seconds
/// plus fewer than this many others. Every host is a credential-derived login,
/// so the org's membership bounds it too.
pub const POST_RATE_SWEEP_AT: usize = 256;

/// What [`PostRates::spend`] decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateSpend {
    Admitted,
    /// Refused. `first` is true on the first refusal since this host's last
    /// admitted post, which is when the refusal is worth a log line.
    Refused {
        first: bool,
    },
}

#[derive(Debug)]
struct PostRate {
    bucket: Bucket,
    refusing: bool,
}

/// Every host's post rate, across all its rooms and connections.
///
/// Keyed by the host the credential resolved to, not by the connection. A
/// host holds one daemon connection at a time (the registry replaces the
/// older one), so the gap a per-connection rate left was reconnecting: each
/// new connection started with a full burst. Held for the life of the router
/// process, so a reconnect inherits the host's rate, including whether its
/// refusals were already logged.
#[derive(Debug)]
pub struct PostRates {
    burst: u64,
    refill: Duration,
    hosts: HashMap<HostId, PostRate>,
}

impl Default for PostRates {
    fn default() -> Self {
        Self::new(POST_BURST, POST_REFILL)
    }
}

impl PostRates {
    pub fn new(burst: u64, refill: Duration) -> Self {
        Self {
            burst,
            refill,
            hosts: HashMap::new(),
        }
    }

    /// Takes one post from `host`'s rate, or refuses without taking anything.
    pub fn spend(&mut self, host: &HostId, now: Instant) -> RateSpend {
        if !self.hosts.contains_key(host) && self.hosts.len() >= POST_RATE_SWEEP_AT {
            self.hosts.retain(|_, rate| !rate.bucket.is_full(now));
        }
        let (burst, refill) = (self.burst, self.refill);
        let rate = self.hosts.entry(host.clone()).or_insert_with(|| PostRate {
            bucket: Bucket::new(burst, refill, now),
            refusing: false,
        });
        if rate.bucket.try_spend(1, now) {
            rate.refusing = false;
            RateSpend::Admitted
        } else {
            let first = !rate.refusing;
            rate.refusing = true;
            RateSpend::Refused { first }
        }
    }

    #[cfg(test)]
    pub(crate) fn tracked(&self) -> usize {
        self.hosts.len()
    }
}

/// Why the token budget refused a post. Server-side detail for the log. On
/// the wire, both are `PostRefusal::BudgetExhausted`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Overspent {
    /// This host's allowance in this room does not cover the post.
    Exhausted,
    /// This host already has [`MAX_ALLOWANCES_PER_HOST`] allowances in use, and
    /// the post would open another.
    TooManyRooms,
}

/// What [`Allowances::spend`] decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Spend {
    Admitted,
    /// Refused. `first` is true on the first refusal since this host last
    /// posted to this room, which is when the refusal is worth a log line.
    Refused {
        why: Overspent,
        first: bool,
    },
}

#[derive(Debug)]
struct Allowance {
    bucket: Bucket,
    refusing: bool,
}

/// Every host's allowance in every room it has recently posted to.
///
/// Held for the life of the router process, not the life of a room or a
/// connection, so neither emptying a room nor reconnecting resets one.
#[derive(Debug)]
pub struct Allowances {
    per_hour: TokensPerHour,
    hosts: HashMap<HostId, BTreeMap<RoomId, Allowance>>,
    /// Hosts refused for [`Overspent::TooManyRooms`] since their last admitted
    /// post, so that refusal is logged once per episode too.
    refusing_new_rooms: HashSet<HostId>,
}

impl Default for Allowances {
    fn default() -> Self {
        Self::new(TokensPerHour::DEFAULT)
    }
}

impl Allowances {
    pub fn new(per_hour: TokensPerHour) -> Self {
        Self {
            per_hour,
            hosts: HashMap::new(),
            refusing_new_rooms: HashSet::new(),
        }
    }

    /// Charges `cost` to `host`'s allowance in `room`, or refuses without
    /// charging anything.
    pub fn spend(&mut self, room: &RoomId, host: &HostId, cost: u64, now: Instant) -> Spend {
        let per_hour = self.per_hour;
        let rooms = self.hosts.entry(host.clone()).or_default();

        if !rooms.contains_key(room) {
            if rooms.len() >= MAX_ALLOWANCES_PER_HOST {
                rooms.retain(|_, allowance| !allowance.bucket.is_full(now));
            }
            if rooms.len() >= MAX_ALLOWANCES_PER_HOST {
                let first = self.refusing_new_rooms.insert(host.clone());
                return Spend::Refused {
                    why: Overspent::TooManyRooms,
                    first,
                };
            }
        }

        let allowance = rooms.entry(room.clone()).or_insert_with(|| Allowance {
            bucket: Bucket::allowance(per_hour, now),
            refusing: false,
        });
        if allowance.bucket.try_spend(cost, now) {
            allowance.refusing = false;
            self.refusing_new_rooms.remove(host);
            Spend::Admitted
        } else {
            let first = !allowance.refusing;
            allowance.refusing = true;
            Spend::Refused {
                why: Overspent::Exhausted,
                first,
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn held_by(&self, host: &HostId) -> usize {
        self.hosts.get(host).map_or(0, BTreeMap::len)
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

    #[test]
    fn a_token_is_three_bytes_of_body_rounded_up_plus_the_envelope() {
        assert_eq!(tokens(&Body::new("x").unwrap()), 1 + ENVELOPE_TOKENS);
        assert_eq!(tokens(&Body::new("abc").unwrap()), 1 + ENVELOPE_TOKENS);
        assert_eq!(tokens(&Body::new("abcd").unwrap()), 2 + ENVELOPE_TOKENS);
        // Three bytes per character: one token each.
        assert_eq!(tokens(&Body::new("漢字").unwrap()), 2 + ENVELOPE_TOKENS);
    }

    #[test]
    fn the_largest_body_costs_exactly_the_largest_post() {
        let widest = Body::new("😀".repeat(MAX_BODY_CHARS)).unwrap();
        assert_eq!(tokens(&widest), MAX_POST_TOKENS);
        // Any other maximum-length body costs no more.
        let ascii = Body::new("a".repeat(MAX_BODY_CHARS)).unwrap();
        assert!(tokens(&ascii) < MAX_POST_TOKENS);
    }

    #[test]
    fn an_unset_allowance_is_the_default() {
        assert_eq!(parse_tokens_per_hour(None), Ok(TokensPerHour::DEFAULT));
    }

    #[test]
    fn a_configured_allowance_inside_the_bounds_is_used() {
        assert_eq!(
            parse_tokens_per_hour(Some(" 50000 ")).unwrap().get(),
            50_000
        );
        assert_eq!(
            parse_tokens_per_hour(Some(&TokensPerHour::MAX.get().to_string())),
            Ok(TokensPerHour::MAX)
        );
        assert_eq!(
            parse_tokens_per_hour(Some(&MAX_POST_TOKENS.to_string())),
            Ok(TokensPerHour::MIN)
        );
    }

    #[test]
    fn no_configuration_can_raise_the_allowance_past_the_ceiling() {
        for over in [TokensPerHour::MAX.get() + 1, u64::MAX] {
            assert_eq!(
                parse_tokens_per_hour(Some(&over.to_string())),
                Err(AllowanceError::AboveCeiling { tokens: over })
            );
            assert_eq!(
                TokensPerHour::new(over),
                Err(AllowanceError::AboveCeiling { tokens: over })
            );
        }
        // Past u64 entirely is not a number, not a wrap.
        assert!(matches!(
            parse_tokens_per_hour(Some("18446744073709551616")),
            Err(AllowanceError::NotANumber { .. })
        ));
    }

    #[test]
    fn an_allowance_that_would_mute_a_room_is_refused() {
        for under in [0, 1, MAX_POST_TOKENS - 1] {
            assert_eq!(
                parse_tokens_per_hour(Some(&under.to_string())),
                Err(AllowanceError::BelowFloor { tokens: under })
            );
        }
    }

    #[test]
    fn a_malformed_allowance_is_an_error_not_a_default() {
        for raw in ["", "lots", "-1", "20k", "1e4"] {
            let error = parse_tokens_per_hour(Some(raw)).unwrap_err();
            assert!(
                error.to_string().contains(TOKENS_PER_HOUR_VAR),
                "{raw:?}: {error}"
            );
        }
    }

    #[test]
    fn a_variable_that_is_not_utf8_is_refused_not_read_as_unset() {
        use std::{env::VarError, ffi::OsString};

        assert_eq!(
            tokens_per_hour_from_env(Err(VarError::NotPresent)),
            Ok(TokensPerHour::DEFAULT)
        );
        assert_eq!(
            tokens_per_hour_from_env(Err(VarError::NotUnicode(OsString::from("x")))),
            Err(AllowanceError::NotUnicode)
        );
        assert_eq!(
            tokens_per_hour_from_env(Ok("30000".to_owned())).map(TokensPerHour::get),
            Ok(30_000)
        );
        assert!(matches!(
            tokens_per_hour_from_env(Ok(String::new())),
            Err(AllowanceError::NotANumber { .. })
        ));
    }

    #[test]
    fn the_refill_interval_is_never_zero() {
        for per_hour in [
            TokensPerHour::MIN,
            TokensPerHour::DEFAULT,
            TokensPerHour::MAX,
        ] {
            assert!(per_hour.per_token() > Duration::ZERO, "{per_hour:?}");
        }
        assert_eq!(
            TokensPerHour::DEFAULT.per_token(),
            Duration::from_millis(180)
        );
    }

    #[test]
    fn a_bucket_refuses_what_it_cannot_cover_and_takes_nothing_for_it() {
        let now = Instant::now();
        let mut bucket = Bucket::new(10, Duration::from_secs(1), now);
        assert!(bucket.try_spend(7, now));
        assert!(!bucket.try_spend(4, now), "spent past the balance");
        // The refused four took nothing, so three still fit.
        assert!(bucket.try_spend(3, now));
        assert!(!bucket.try_spend(1, now));
    }

    #[test]
    fn a_bucket_refills_one_per_interval_and_never_past_capacity() {
        let start = Instant::now();
        let mut bucket = Bucket::new(2, Duration::from_secs(2), start);
        assert!(bucket.try_spend(2, start));
        assert!(!bucket.try_spend(1, start + Duration::from_millis(1_999)));
        assert!(bucket.try_spend(1, start + Duration::from_secs(2)));
        assert!(!bucket.try_spend(1, start + Duration::from_secs(3)));
        // A long idle stretch fills it to capacity and no further.
        let later = start + Duration::from_secs(3600);
        assert!(bucket.is_full(later));
        assert!(bucket.try_spend(2, later));
        assert!(!bucket.try_spend(1, later));
    }

    #[test]
    fn a_partial_interval_is_kept_rather_than_lost_between_calls() {
        let start = Instant::now();
        let mut bucket = Bucket::new(1, Duration::from_secs(2), start);
        assert!(bucket.try_spend(1, start));
        // Asking every 1.5s: the half second left over each time must carry.
        assert!(!bucket.try_spend(1, start + Duration::from_millis(1_500)));
        assert!(bucket.try_spend(1, start + Duration::from_millis(3_000)));
    }

    #[test]
    fn a_bucket_survives_extreme_clocks_and_intervals() {
        let start = Instant::now();
        // A clock that reads earlier than the last refill earns nothing.
        let mut early = Bucket::new(1, Duration::from_secs(1), start + Duration::from_secs(60));
        assert!(early.try_spend(1, start));
        assert!(!early.try_spend(1, start));

        // A zero interval is instant refill, not a division by zero.
        let mut zero = Bucket::new(3, Duration::ZERO, start);
        assert!(zero.try_spend(3, start));
        assert!(zero.try_spend(3, start + Duration::from_nanos(1)));

        // An interval and an elapsed time at the ends of their ranges.
        let mut huge = Bucket::new(u64::MAX, Duration::MAX, start);
        assert!(huge.try_spend(u64::MAX, start));
        assert!(!huge.try_spend(1, start + Duration::from_secs(u64::from(u32::MAX))));
        let mut fast = Bucket::new(u64::MAX, Duration::from_nanos(1), start);
        assert!(fast.try_spend(u64::MAX, start));
        assert!(fast.try_spend(1, start + Duration::from_secs(u64::from(u32::MAX))));
    }

    #[test]
    fn a_post_rate_is_per_host_and_only_a_refilled_one_is_forgotten() {
        let start = Instant::now();
        let mut rates = PostRates::default();
        let atlas = host("atlas");
        for _ in 0..POST_BURST {
            assert_eq!(rates.spend(&atlas, start), RateSpend::Admitted);
        }
        assert_eq!(
            rates.spend(&atlas, start),
            RateSpend::Refused { first: true }
        );
        assert_eq!(
            rates.spend(&atlas, start),
            RateSpend::Refused { first: false },
            "a repeat refusal was reported as the first"
        );
        assert_eq!(rates.spend(&host("bay"), start), RateSpend::Admitted);
        assert_eq!(
            rates.spend(&atlas, start + POST_REFILL),
            RateSpend::Admitted
        );
        assert_eq!(
            rates.spend(&atlas, start + POST_REFILL),
            RateSpend::Refused { first: true }
        );

        // Enough other hosts to trigger a sweep, each of which posted once and
        // has refilled by `later`. atlas has not.
        for n in 0..POST_RATE_SWEEP_AT {
            rates.spend(&host(&format!("h{n}")), start);
        }
        let later = start + POST_REFILL * 2;
        assert_eq!(rates.spend(&host("newcomer"), later), RateSpend::Admitted);
        assert_eq!(
            rates.tracked(),
            2,
            "the sweep kept a refilled rate or forgot atlas's spent one"
        );
        // One post earned since her last; a forgotten rate would be a burst.
        assert_eq!(rates.spend(&atlas, later), RateSpend::Admitted);
        assert!(matches!(
            rates.spend(&atlas, later),
            RateSpend::Refused { .. }
        ));
    }

    #[test]
    fn an_allowance_is_per_room_and_per_host() {
        let now = Instant::now();
        let mut allowances = Allowances::new(TokensPerHour::MIN);
        let all = TokensPerHour::MIN.get();

        assert_eq!(
            allowances.spend(&ticket(1), &host("atlas"), all, now),
            Spend::Admitted
        );
        assert_eq!(
            allowances.spend(&ticket(1), &host("atlas"), 1, now),
            Spend::Refused {
                why: Overspent::Exhausted,
                first: true
            }
        );
        assert_eq!(
            allowances.spend(&ticket(1), &host("atlas"), 1, now),
            Spend::Refused {
                why: Overspent::Exhausted,
                first: false
            },
            "a repeat refusal was reported as the first"
        );
        // Another room, same host.
        assert_eq!(
            allowances.spend(&ticket(2), &host("atlas"), all, now),
            Spend::Admitted
        );
        // Same room, another host.
        assert_eq!(
            allowances.spend(&ticket(1), &host("bay"), all, now),
            Spend::Admitted
        );
    }

    #[test]
    fn a_refused_host_is_reported_first_again_after_an_admitted_post() {
        let start = Instant::now();
        let mut allowances = Allowances::new(TokensPerHour::MIN);
        let atlas = host("atlas");
        let all = TokensPerHour::MIN.get();
        allowances.spend(&ticket(1), &atlas, all, start);
        assert!(matches!(
            allowances.spend(&ticket(1), &atlas, 1, start),
            Spend::Refused { first: true, .. }
        ));
        let refilled = start + HOUR;
        assert_eq!(
            allowances.spend(&ticket(1), &atlas, all, refilled),
            Spend::Admitted
        );
        assert!(matches!(
            allowances.spend(&ticket(1), &atlas, 1, refilled),
            Spend::Refused { first: true, .. }
        ));
    }

    #[test]
    fn one_host_cannot_hold_more_allowances_than_the_cap_and_a_refilled_one_is_forgotten() {
        let start = Instant::now();
        let mut allowances = Allowances::new(TokensPerHour::DEFAULT);
        let atlas = host("atlas");
        let cap = u32::try_from(MAX_ALLOWANCES_PER_HOST).unwrap();
        for n in 1..=cap {
            assert_eq!(
                allowances.spend(&ticket(n), &atlas, 1, start),
                Spend::Admitted
            );
        }
        assert_eq!(
            allowances.spend(&ticket(cap + 1), &atlas, 1, start),
            Spend::Refused {
                why: Overspent::TooManyRooms,
                first: true
            }
        );
        assert!(matches!(
            allowances.spend(&ticket(cap + 1), &atlas, 1, start),
            Spend::Refused {
                why: Overspent::TooManyRooms,
                first: false
            }
        ));
        // A room already held can still be posted to.
        assert_eq!(
            allowances.spend(&ticket(1), &atlas, 1, start),
            Spend::Admitted
        );
        // bay's slots are his own.
        assert_eq!(
            allowances.spend(&ticket(cap + 1), &host("bay"), 1, start),
            Spend::Admitted
        );
        assert_eq!(allowances.held_by(&atlas), MAX_ALLOWANCES_PER_HOST);

        // Once they have refilled, atlas's allowances carry nothing and make
        // way for a new room.
        let refilled = start + HOUR;
        assert_eq!(
            allowances.spend(&ticket(cap + 1), &atlas, 1, refilled),
            Spend::Admitted
        );
        assert_eq!(allowances.held_by(&atlas), 1);
    }
}
