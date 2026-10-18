//! A per-process, per-tool rate limit (rule 24).
//!
//! Each tool that reaches something another party pays for holds one
//! `static` [`Cooldown`], shared by every call in this process. It is a
//! static rather than state on the server because each call gets a fresh
//! `RouterAccess`, and anything held on that would reset every time an agent
//! called again. A limit an agent can re-call past is not a limit.

use std::{
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};

/// When a call last went through. Claimed at the start of a call, not its
/// end, so calls made in parallel cannot all pass before the first one
/// finishes.
pub(crate) struct Cooldown {
    window: Duration,
    last: Mutex<Option<Instant>>,
}

impl Cooldown {
    pub(crate) const fn new(window: Duration) -> Self {
        Self {
            window,
            last: Mutex::new(None),
        }
    }

    /// Whether a call at `now` may go through, recording it if so. A refused
    /// call does not move the window. A reading earlier than the last one is
    /// refused rather than trusted.
    pub(crate) fn claim(&self, now: Instant) -> bool {
        // Nothing panics while holding this lock, but a poisoned lock still
        // guards a valid `Option<Instant>`, so it is used rather than unwrapped.
        let mut last = self.last.lock().unwrap_or_else(PoisonError::into_inner);
        if last.is_some_and(|previous| now.saturating_duration_since(previous) < self.window) {
            return false;
        }
        *last = Some(now);
        true
    }
}
