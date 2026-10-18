//! What other machines' answers may spend of one reader's GitHub token
//!.
//!
//! Narrowing a fan-out checks every agent row's `repo` against the reader's
//! own token, and that `repo` comes from a daemon's snapshot, which is hostile
//! input. A daemon that names fresh repos in every answer misses the access
//! cache every time, so each of its answers costs the *reader* GitHub calls.
//! `MAX_REPOS_PER_QUERY` bounds one query; this module bounds the rate across
//! queries (rules 24, 44).
//!
//! # Per reader, keyed by the credential
//!
//! Each reader has a refilling allowance of uncached checks: [`CHECK_BURST`] at
//! once, then [`ChecksPerHour`]. The key is the host the reader's credential
//! resolved to at its handshake, never anything a snapshot or a frame carried
//! (rule 12). It is not keyed by the answering daemon as well: the resource
//! being protected is the reader's GitHub rate limit, and a per-daemon
//! allowance would hand every additional hostile daemon a fresh one.
//!
//! # Only a GitHub call is charged
//!
//! An answer the access cache already holds costs nothing, so a reader whose
//! fleet keeps working in the same repos never notices this limit. The charge
//! is taken immediately before the call it pays for, never after.
//!
//! # Refusing fails closed
//!
//! A check the allowance cannot cover is not made. Its repo is withheld exactly
//! like one GitHub refused, and the query answers `Partial` (core rule 38). An
//! exhausted allowance never reads as readable, and never as a machine running
//! nothing without the notice.
//!
//! # In-process
//!
//! Allowances live in this router process and reset on deploy, like the
//! access cache they sit beside.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use sealmesh_wire::HostId;

use crate::room_budget::Bucket;

/// The environment variable that sets each reader's refill rate.
pub const CHECKS_PER_HOUR_VAR: &str = "SEALMESH_READER_CHECKS_PER_HOUR";

/// Uncached checks one reader may spend at once before the refill rate
/// applies: two cold-cache queries at `MAX_REPOS_PER_QUERY`.
pub const CHECK_BURST: u64 = 64;

/// Most readers' allowances held at once.
///
/// A full allowance is the same as a fresh one, so it is forgotten once the
/// table is full — at the default that is under four minutes after a reader's
/// last uncached check. Every key is a credential-derived org member, so this
/// is far above the org. A reader new to a table still full after that sweep
/// is refused its checks rather than granted them: withheld and `Partial`.
pub const MAX_TRACKED_READERS: usize = 1024;

const HOUR: Duration = Duration::from_secs(60 * 60);

/// Uncached access checks one reader's allowance earns back per hour.
///
/// The only constructor refuses a value outside
/// [`MIN`](Self::MIN)..=[`MAX`](Self::MAX), so no configuration can raise it
/// past the ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChecksPerHour(u64);

impl ChecksPerHour {
    /// One check every 3.6 seconds: a fifth of GitHub's 5,000-an-hour
    /// authenticated limit, which a hostile daemon can therefore never take
    /// more of. A reader asking continuously is still covered for about eight
    /// distinct repos, since the 30-second access cache caps each repo at 120
    /// checks an hour however often it is asked.
    pub const DEFAULT: Self = Self(1_000);

    /// Two fifths of GitHub's limit. Above this, a hostile daemon could take
    /// close to half of a teammate's hourly allowance, and the budget would
    /// stop protecting it.
    pub const MAX: Self = Self(2_000);

    /// One burst an hour. Below this, an emptied allowance would take longer
    /// than an hour to come back, which reads as a broken router.
    pub const MIN: Self = Self(CHECK_BURST);

    pub fn new(checks: u64) -> Result<Self, CheckBudgetError> {
        if checks < Self::MIN.0 {
            return Err(CheckBudgetError::BelowFloor { checks });
        }
        if checks > Self::MAX.0 {
            return Err(CheckBudgetError::AboveCeiling { checks });
        }
        Ok(Self(checks))
    }

    pub fn get(self) -> u64 {
        self.0
    }

    /// How long a reader waits to earn back one check. Never zero, so it can
    /// never be divided by (rule 40).
    fn per_check(self) -> Duration {
        let nanos = HOUR.as_nanos().checked_div(u128::from(self.0)).unwrap_or(0);
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX).max(1))
    }
}

impl Default for ChecksPerHour {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CheckBudgetError {
    #[error("{CHECKS_PER_HOUR_VAR} is not valid UTF-8")]
    NotUnicode,
    #[error("{CHECKS_PER_HOUR_VAR} is not a whole number of checks: {raw:?}")]
    NotANumber { raw: String },
    #[error(
        "{CHECKS_PER_HOUR_VAR}={checks} is below the floor of {CHECK_BURST}, one burst an hour"
    )]
    BelowFloor { checks: u64 },
    #[error("{CHECKS_PER_HOUR_VAR}={checks} exceeds the hard ceiling of {}", ChecksPerHour::MAX.0)]
    AboveCeiling { checks: u64 },
}

/// The configured rate, or the default when the variable is unset.
///
/// A malformed or out-of-range value is refused, so the router does not start,
/// rather than being clamped. This follows `SEALMESH_ROOM_TOKENS_PER_HOUR`,
/// which refuses both non-UTF-8 and zero the same way. The value is the
/// operator's own, so quoting it back in the error is safe (rule 14 is about
/// peers).
pub fn parse_checks_per_hour(raw: Option<&str>) -> Result<ChecksPerHour, CheckBudgetError> {
    let Some(raw) = raw else {
        return Ok(ChecksPerHour::DEFAULT);
    };
    let checks: u64 = raw
        .trim()
        .parse()
        .map_err(|_| CheckBudgetError::NotANumber {
            raw: raw.to_owned(),
        })?;
    ChecksPerHour::new(checks)
}

/// [`parse_checks_per_hour`] for the variable as `std::env::var` returned it.
/// Only an unset variable means the default.
pub fn checks_per_hour_from_env(
    value: Result<String, std::env::VarError>,
) -> Result<ChecksPerHour, CheckBudgetError> {
    match value {
        Ok(raw) => parse_checks_per_hour(Some(&raw)),
        Err(std::env::VarError::NotPresent) => Ok(ChecksPerHour::DEFAULT),
        Err(std::env::VarError::NotUnicode(_)) => Err(CheckBudgetError::NotUnicode),
    }
}

/// Every reader's allowance of uncached access checks.
#[derive(Debug)]
pub struct CheckBudgets {
    per_hour: ChecksPerHour,
    readers: Mutex<HashMap<HostId, Bucket>>,
}

impl Default for CheckBudgets {
    fn default() -> Self {
        Self::new(ChecksPerHour::DEFAULT)
    }
}

impl CheckBudgets {
    pub fn new(per_hour: ChecksPerHour) -> Self {
        Self {
            per_hour,
            readers: Mutex::new(HashMap::new()),
        }
    }

    pub fn per_hour(&self) -> ChecksPerHour {
        self.per_hour
    }

    /// Takes one check from `reader`'s allowance, or refuses without taking
    /// anything. A refusal means the check must not be made. It refuses when
    /// `reader`'s allowance is spent, and when `reader` is new and the table
    /// already holds [`MAX_TRACKED_READERS`] allowances that have not refilled.
    pub fn try_charge(&self, reader: &HostId) -> bool {
        self.try_charge_at(reader, Instant::now())
    }

    fn try_charge_at(&self, reader: &HostId, now: Instant) -> bool {
        // Recover rather than panic on a poisoned lock (rule 39). Nothing in
        // this function can panic while holding it, so the state is whole.
        let mut readers = self
            .readers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if !readers.contains_key(reader) {
            if readers.len() >= MAX_TRACKED_READERS {
                readers.retain(|_, bucket| !bucket.is_full(now));
            }
            if readers.len() >= MAX_TRACKED_READERS {
                return false;
            }
        }

        let per_check = self.per_hour.per_check();
        readers
            .entry(reader.clone())
            .or_insert_with(|| Bucket::new(CHECK_BURST, per_check, now))
            .try_spend(1, now)
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.readers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn host(name: &str) -> HostId {
        HostId::try_from(name).unwrap()
    }

    #[test]
    fn an_unset_rate_is_the_default() {
        assert_eq!(parse_checks_per_hour(None), Ok(ChecksPerHour::DEFAULT));
        assert_eq!(
            checks_per_hour_from_env(Err(std::env::VarError::NotPresent)),
            Ok(ChecksPerHour::DEFAULT)
        );
    }

    #[test]
    fn a_configured_rate_inside_the_bounds_is_used() {
        assert_eq!(parse_checks_per_hour(Some(" 1500 ")).unwrap().get(), 1_500);
        assert_eq!(
            parse_checks_per_hour(Some(&ChecksPerHour::MAX.get().to_string())),
            Ok(ChecksPerHour::MAX)
        );
        assert_eq!(
            parse_checks_per_hour(Some(&CHECK_BURST.to_string())),
            Ok(ChecksPerHour::MIN)
        );
    }

    #[test]
    fn no_configuration_can_raise_the_rate_past_the_ceiling() {
        for over in [ChecksPerHour::MAX.get() + 1, u64::MAX] {
            assert_eq!(
                parse_checks_per_hour(Some(&over.to_string())),
                Err(CheckBudgetError::AboveCeiling { checks: over })
            );
            assert_eq!(
                ChecksPerHour::new(over),
                Err(CheckBudgetError::AboveCeiling { checks: over })
            );
        }
        assert!(matches!(
            parse_checks_per_hour(Some("18446744073709551616")),
            Err(CheckBudgetError::NotANumber { .. })
        ));
    }

    #[test]
    fn a_rate_below_one_burst_an_hour_is_refused_including_zero() {
        for under in [0, 1, CHECK_BURST - 1] {
            assert_eq!(
                parse_checks_per_hour(Some(&under.to_string())),
                Err(CheckBudgetError::BelowFloor { checks: under })
            );
        }
    }

    #[test]
    fn a_malformed_rate_is_an_error_not_a_default() {
        for raw in ["", "lots", "-1", "1k", "1e3"] {
            let error = parse_checks_per_hour(Some(raw)).unwrap_err();
            assert!(
                error.to_string().contains(CHECKS_PER_HOUR_VAR),
                "{raw:?}: {error}"
            );
        }
        assert_eq!(
            checks_per_hour_from_env(Err(std::env::VarError::NotUnicode("x".into()))),
            Err(CheckBudgetError::NotUnicode)
        );
    }

    #[test]
    fn the_refill_interval_is_never_zero_and_the_ceiling_bounds_it() {
        for rate in [
            ChecksPerHour::MIN,
            ChecksPerHour::DEFAULT,
            ChecksPerHour::MAX,
        ] {
            assert!(rate.per_check() > Duration::ZERO, "{rate:?}");
            assert!(rate.per_check() >= ChecksPerHour::MAX.per_check());
        }
        assert_eq!(
            ChecksPerHour::DEFAULT.per_check(),
            Duration::from_millis(3_600)
        );
    }

    #[test]
    fn a_reader_gets_one_burst_then_one_check_per_interval() {
        let start = Instant::now();
        let budgets = CheckBudgets::new(ChecksPerHour::DEFAULT);
        let reader = host("reader");
        for _ in 0..CHECK_BURST {
            assert!(budgets.try_charge_at(&reader, start));
        }
        assert!(
            !budgets.try_charge_at(&reader, start),
            "spent past the burst"
        );
        let interval = ChecksPerHour::DEFAULT.per_check();
        assert!(!budgets.try_charge_at(&reader, start + interval / 2));
        assert!(budgets.try_charge_at(&reader, start + interval));
        assert!(!budgets.try_charge_at(&reader, start + interval));
    }

    #[test]
    fn one_readers_spent_allowance_is_not_another_readers() {
        let now = Instant::now();
        let budgets = CheckBudgets::new(ChecksPerHour::DEFAULT);
        for _ in 0..CHECK_BURST {
            budgets.try_charge_at(&host("atlas"), now);
        }
        assert!(!budgets.try_charge_at(&host("atlas"), now));
        assert!(budgets.try_charge_at(&host("bay"), now));
    }

    #[test]
    fn the_table_is_bounded_and_forgets_only_refilled_allowances() {
        let start = Instant::now();
        let budgets = CheckBudgets::new(ChecksPerHour::DEFAULT);
        let spender = host("spender");
        for _ in 0..CHECK_BURST {
            assert!(budgets.try_charge_at(&spender, start));
        }
        for n in 1..MAX_TRACKED_READERS {
            assert!(budgets.try_charge_at(&host(&format!("r{n}")), start));
        }
        assert_eq!(budgets.tracked(), MAX_TRACKED_READERS);

        // Full, and nobody has refilled: a newcomer is refused, not granted,
        // and nobody is forgotten to make room.
        assert!(!budgets.try_charge_at(&host("newcomer"), start));
        assert_eq!(budgets.tracked(), MAX_TRACKED_READERS);

        // One interval later every reader that spent one check has refilled;
        // `spender`, who spent a whole burst, has not.
        let later = start + ChecksPerHour::DEFAULT.per_check();
        assert!(budgets.try_charge_at(&host("newcomer"), later));
        assert_eq!(
            budgets.tracked(),
            2,
            "kept a refilled allowance or lost a spent one"
        );
        assert!(budgets.try_charge_at(&spender, later));
        assert!(
            !budgets.try_charge_at(&spender, later),
            "a forgotten allowance would have been a fresh burst"
        );
    }

    #[test]
    fn extreme_clocks_neither_panic_nor_grant() {
        let start = Instant::now();
        let budgets = CheckBudgets::new(ChecksPerHour::MIN);
        let reader = host("reader");
        let interval = ChecksPerHour::MIN.per_check();
        for _ in 0..CHECK_BURST {
            assert!(budgets.try_charge_at(&reader, start));
        }
        // One interval on, exactly one check has been earned and is spent.
        assert!(budgets.try_charge_at(&reader, start + interval));
        // A clock that then reads earlier than that earns nothing back.
        assert!(!budgets.try_charge_at(&reader, start));
        assert!(!budgets.try_charge_at(&reader, start + interval));

        // Far past any refill: full again, never more than one burst, and no
        // overflow on the way.
        let far = start + Duration::from_secs(u64::from(u32::MAX));
        for _ in 0..CHECK_BURST {
            assert!(budgets.try_charge_at(&reader, far));
        }
        assert!(!budgets.try_charge_at(&reader, far));
    }
}
