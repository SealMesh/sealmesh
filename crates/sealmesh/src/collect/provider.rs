//! What a source of agents has to be, so that adding one does not mean editing
//! the funnel.
//!
//! A provider answers two questions and nothing else: *what sessions can you
//! prove are alive*, and *what agents do you see*. It cannot answer "and this
//! one should be published", because the only thing it is allowed to return is
//! a `Candidate` and it never sees the `Gate`. That is the property the whole
//! design rests on and it survives an open provider set unchanged.

use std::{borrow::Cow, collections::BTreeSet, fmt};

use jiff::Timestamp;

use super::{Candidate, liveness::Liveness};
use crate::wire::ToolId;

/// Who wins when two providers see the same agent.
///
/// This used to be the declaration order of an enum, which works for a closed
/// set of three and cannot work for a set that config and other people extend.
/// Gaps are deliberate: they leave room to insert a provider between two that
/// already exist without renumbering either.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Priority(pub u16);

impl Priority {
    /// A job file is the richest thing on the machine — it has children, so it
    /// is the only source that can name a PR.
    pub const CLAUDE_JOB: Self = Self(300);
    /// A writer that named itself beats one this machine merely inferred.
    pub const DECLARED: Self = Self(200);
    /// Below `DECLARED` and above `CLAUDE_SESSION`: a tool added by config
    /// should lose to a first-class provider that knows more about its subject.
    pub const GENERIC: Self = Self(150);
    /// A session file mostly proves a process exists.
    pub const CLAUDE_SESSION: Self = Self(100);
}

/// Which adapter saw a row. Local only — `doctor` prints it, the wire does not
/// carry it. Distinct from `ToolId`, which says *what the row is* rather than
/// *how we found out*.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProviderId(Cow<'static, str>);

impl ProviderId {
    pub const CLAUDE_JOB: Self = Self(Cow::Borrowed("job"));
    pub const CLAUDE_SESSION: Self = Self(Cow::Borrowed("session"));
    pub const DECLARED: Self = Self(Cow::Borrowed("declared"));

    /// A provider declared in config. The prefix keeps a config-added name from
    /// colliding with a built-in one in `doctor`'s output.
    pub fn generic(name: &str) -> Self {
        Self(Cow::Owned(format!("toml:{name}")))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProviderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A session one provider can vouch for, so another provider's record can name
/// it as a parent. Carrying the tool makes the set describable in `doctor`
/// without changing what a lookup matches on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRef {
    pub tool: ToolId,
    pub id: String,
}

/// Everything a scan is allowed to know about the machine it is running on.
pub struct ScanCtx<'a> {
    pub now: Timestamp,
    pub procs: &'a dyn Liveness,
    /// Every provider's live sessions, collected before any scan runs. A
    /// declared record naming a parent is matched against this, which is why a
    /// non-Claude tool's session can keep a record alive.
    pub live_sessions: &'a BTreeSet<String>,
}

pub trait Provider {
    fn id(&self) -> ProviderId;

    fn priority(&self) -> Priority;

    /// Sessions this provider can prove are alive right now. Runs for every
    /// provider before any `scan`, because a provider that consumes the live
    /// set must not depend on the order providers happen to be listed in.
    fn live_sessions(&self, _procs: &dyn Liveness) -> Vec<SessionRef> {
        Vec::new()
    }

    fn scan(&self, ctx: &ScanCtx<'_>) -> Vec<Candidate>;
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use super::*;

    #[test]
    fn the_built_in_order_is_the_one_the_enum_used_to_encode() {
        // job > declared > session, which is what `Source`'s derived Ord meant
        // before it was a number. Written down because the relation is now a
        // choice rather than a side effect of declaration order.
        assert!(Priority::CLAUDE_JOB > Priority::DECLARED);
        assert!(Priority::DECLARED > Priority::GENERIC);
        assert!(Priority::GENERIC > Priority::CLAUDE_SESSION);
    }

    #[test]
    fn a_config_added_provider_cannot_impersonate_a_built_in_one() {
        for built_in in [
            ProviderId::CLAUDE_JOB,
            ProviderId::CLAUDE_SESSION,
            ProviderId::DECLARED,
        ] {
            assert_ne!(ProviderId::generic(built_in.as_str()), built_in);
        }
        assert_eq!(ProviderId::generic("qwen").as_str(), "toml:qwen");
    }
}
