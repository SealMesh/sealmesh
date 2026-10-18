//! A one-line progress indicator for the stretches where `who` has nothing to
//! print yet.
//!
//! `who` is silent from launch until the final render, and that run is four
//! serial waits: `gh auth token` as a subprocess, the TLS/WebSocket connect,
//! the router's fan-out — where every attached daemon runs a full `gix` status
//! across every allowed checkout before it answers — and pull request
//! enrichment, bounded by an eight second budget. Nothing in that wait
//! distinguishes "`gh` is broken" from "the router is down" from "no daemon is
//! attached" from "GitHub is slow".
//!
//! Two of the phases carry a real quantity: `Report` frames arrive one per
//! machine until `Complete`, and pull request lookups are a known n of m. So
//! this is progress reporting that happens to animate, rather than a spinner
//! painted over an opaque wait.

use std::{
    ffi::OsStr,
    io::{IsTerminal, Write as _},
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::task::JoinHandle;

use crate::render;

/// How often the line repaints. Fast enough to read as motion, slow enough
/// that a `who` against a slow router is not also a busy loop.
const TICK: Duration = Duration::from_millis(100);

const SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Where the caller is in the query, and how far along.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Connecting,
    Asking { answered: usize },
    Resolving { done: usize, total: usize },
}

/// One rendered line, without the escape sequences that position it — those
/// live in the surface, so this stays a pure function a test can assert
/// against with no terminal anywhere.
///
/// Every line this produces is well under eighty columns on purpose. A `\r`
/// returns to the start of the *current* line, so a line long enough to wrap
/// leaves the overflow behind when the next repaint erases only one line, and
/// reading the terminal's width would mean either an `ioctl` or a dependency.
/// Staying short is the cheaper guarantee.
pub fn frame(phase: Phase, tick: u64) -> String {
    let spin = spinner(tick);
    match phase {
        Phase::Connecting => format!("{spin} connecting to the router"),
        Phase::Asking { answered: 0 } => format!("{spin} asking every machine"),
        Phase::Asking { answered } => {
            format!("{spin} asking every machine · {answered} answered")
        }
        Phase::Resolving { done, total } => format!(
            "{spin} pull requests {} {done}/{total}",
            render::bar(done, total)
        ),
    }
}

fn spinner(tick: u64) -> &'static str {
    let index = usize::try_from(tick % 10).unwrap_or(0);
    SPINNER.get(index).copied().unwrap_or("·")
}

/// Where a painted line goes. A trait rather than a hardcoded `stderr` so the
/// cleanup guarantee — that nothing paints after `finish` — is testable
/// without a terminal.
trait Surface: Send {
    fn paint(&mut self, line: &str);
    fn erase(&mut self);
}

/// The real one.
///
/// Writes to **stderr**, never stdout: `who`'s stdout is what people pipe into
/// `grep` or `less`, and progress bytes landing in a pipe would corrupt it.
///
/// It never hides the cursor. `ESC[?25l` with no signal handler leaves a
/// Ctrl-C'd terminal with no cursor, permanently, and restoring it would mean
/// owning a signal handler for the sake of a cosmetic flicker.
struct Stderr;

impl Surface for Stderr {
    fn paint(&mut self, line: &str) {
        let mut err = std::io::stderr().lock();
        let _ = write!(err, "\r{line}\u{1b}[K");
        let _ = err.flush();
    }

    fn erase(&mut self) {
        let mut err = std::io::stderr().lock();
        let _ = write!(err, "\r\u{1b}[K");
        let _ = err.flush();
    }
}

/// Phase, tick and surface under one lock. Every write to the terminal happens
/// while holding it, so a `note` cannot land in the middle of a repaint. There
/// is no `.await` inside the critical section, so the lock is never held
/// across one.
struct Painter {
    phase: Phase,
    tick: u64,
    surface: Box<dyn Surface>,
}

struct Inner {
    painter: Arc<Mutex<Painter>>,
    handle: JoinHandle<()>,
}

pub struct Progress {
    inner: Option<Inner>,
}

impl Progress {
    /// Starts painting, or returns a handle that does nothing at all when this
    /// is not an interactive terminal.
    pub fn start() -> Self {
        if enabled() {
            Self::spawn(Box::new(Stderr))
        } else {
            Self { inner: None }
        }
    }

    fn spawn(surface: Box<dyn Surface>) -> Self {
        let painter = Arc::new(Mutex::new(Painter {
            phase: Phase::Connecting,
            tick: 0,
            surface,
        }));
        let ticking = Arc::clone(&painter);
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(TICK);
            // `interval`'s first tick completes immediately, which would paint
            // a frame on a run that finishes in under a tick — a `who` with a
            // warm cache blinks a spinner and erases it. Spending that tick
            // here makes the first paint land one period in, so a fast command
            // stays silent, which is the whole point of the gate.
            interval.tick().await;
            loop {
                interval.tick().await;
                let Ok(mut painter) = ticking.lock() else {
                    return;
                };
                painter.tick = painter.tick.wrapping_add(1);
                let line = frame(painter.phase, painter.tick);
                painter.surface.paint(&line);
            }
        });
        Self {
            inner: Some(Inner { painter, handle }),
        }
    }

    /// Moves to a new phase. The next repaint picks it up; this never writes.
    pub fn set(&self, phase: Phase) {
        let Some(inner) = &self.inner else {
            return;
        };
        if let Ok(mut painter) = inner.painter.lock() {
            painter.phase = phase;
        }
    }

    /// Prints a message that has to survive, without the live line landing in
    /// the middle of it. The next tick repaints below it.
    pub fn note(&self, message: &str) {
        let Some(inner) = &self.inner else {
            eprintln!("{message}");
            return;
        };
        let Ok(mut painter) = inner.painter.lock() else {
            eprintln!("{message}");
            return;
        };
        painter.surface.erase();
        eprintln!("{message}");
    }

    /// Stops the painter and clears the line.
    ///
    /// This joins the task before erasing, which is the whole ordering
    /// guarantee: once `finish` returns, no repaint can land on top of the
    /// report the caller is about to write to stdout. `abort` can only take
    /// effect at an await point, and the only await is the interval tick —
    /// outside the lock — so it can never land mid-paint or poison the mutex.
    pub async fn finish(self) {
        let Some(inner) = self.inner else {
            return;
        };
        inner.handle.abort();
        let _ = inner.handle.await;
        if let Ok(mut painter) = inner.painter.lock() {
            painter.surface.erase();
        }
    }
}

fn enabled() -> bool {
    decide(
        std::io::stderr().is_terminal(),
        std::env::var_os("NO_COLOR").as_deref(),
        std::env::var_os("TERM").as_deref(),
    )
}

/// The gate, split out from the environment so it can be tested without
/// mutating process-global state.
///
/// The *stream* check is on stderr, deliberately not reusing `who`'s colour
/// check — that one asks whether *stdout* is a terminal, and
/// `sealmesh who | less` has a piped stdout and a terminal stderr. The
/// *environment* half is `render::styling_allowed_in`, shared with the report's
/// own colour so `NO_COLOR` cannot silence one surface and not the other.
fn decide(stderr_is_terminal: bool, no_color: Option<&OsStr>, term: Option<&OsStr>) -> bool {
    stderr_is_terminal && render::styling_allowed_in(no_color, term)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[derive(Clone, Default)]
    struct Recorder(Arc<Mutex<Vec<String>>>);

    impl Recorder {
        fn writes(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }
    }

    impl Surface for Recorder {
        fn paint(&mut self, line: &str) {
            self.0.lock().unwrap().push(format!("paint:{line}"));
        }

        fn erase(&mut self) {
            self.0.lock().unwrap().push("erase".to_owned());
        }
    }

    #[test]
    fn the_spinner_cycles_and_cannot_drift_from_its_modulus() {
        assert_eq!(SPINNER.len(), 10, "spinner(tick) takes tick % 10");
        assert_eq!(spinner(0), spinner(10));
        assert_ne!(spinner(0), spinner(1));
        assert_eq!(spinner(u64::MAX), spinner(u64::MAX % 10));
    }

    #[test]
    fn every_phase_renders_something_a_person_can_read() {
        assert!(frame(Phase::Connecting, 0).contains("router"));
        assert!(frame(Phase::Asking { answered: 0 }, 0).contains("asking"));

        let answered = frame(Phase::Asking { answered: 3 }, 0);
        assert!(answered.contains('3'), "{answered}");
        assert!(answered.contains("answered"), "{answered}");

        let resolving = frame(Phase::Resolving { done: 2, total: 5 }, 0);
        assert!(resolving.contains("2/5"), "{resolving}");
    }

    #[test]
    fn the_countable_phase_reuses_the_one_progress_bar_this_binary_has() {
        let rendered = frame(Phase::Resolving { done: 4, total: 8 }, 0);
        assert!(rendered.contains(&render::bar(4, 8)), "{rendered}");
    }

    #[test]
    fn a_frame_carries_no_control_characters_of_its_own() {
        // Positioning belongs to the surface. A frame that smuggled an escape
        // sequence in would move the cursor somewhere `finish` cannot erase.
        for phase in [
            Phase::Connecting,
            Phase::Asking { answered: 12 },
            Phase::Resolving { done: 3, total: 9 },
        ] {
            let rendered = frame(phase, 7);
            assert!(
                !rendered.chars().any(char::is_control),
                "{rendered:?} carries a control character"
            );
        }
    }

    #[test]
    fn no_line_can_wrap_an_eighty_column_terminal() {
        // `\r` only rewinds the current line, so a wrapped line would leave
        // residue that no erase reaches.
        for phase in [
            Phase::Connecting,
            Phase::Asking {
                answered: usize::MAX,
            },
            Phase::Resolving {
                done: 999,
                total: 999,
            },
        ] {
            let width = frame(phase, 0).chars().count();
            assert!(width < 80, "{width} columns is too wide");
        }
    }

    #[test]
    fn the_gate_is_closed_whenever_stderr_is_not_an_interactive_terminal() {
        assert!(decide(true, None, None));
        assert!(!decide(false, None, None), "piped stderr");
        assert!(!decide(true, Some(OsStr::new("1")), None), "NO_COLOR");
        assert!(!decide(true, None, Some(OsStr::new("dumb"))), "TERM=dumb");

        // The NO_COLOR convention is that an empty value does not count.
        assert!(decide(
            true,
            Some(OsStr::new("")),
            Some(OsStr::new("xterm"))
        ));
    }

    #[test]
    fn a_disabled_handle_needs_no_runtime_at_all() {
        // A plain `#[test]` has no reactor, so this fails the moment `set` or
        // `note` reaches for a task handle instead of short-circuiting on
        // `inner: None`. The note's text goes to the real stderr through the
        // plain `eprintln!` fallback, which is why it is not asserted here.
        let progress = Progress { inner: None };
        progress.set(Phase::Asking { answered: 1 });
        progress.note("this still has to reach the operator");
    }

    #[tokio::test]
    async fn a_disabled_handle_finishes_without_a_runtime_task() {
        Progress { inner: None }.finish().await;
    }

    #[tokio::test]
    async fn nothing_paints_after_finish_returns() {
        // The failure this guards: a repaint landing on top of the report
        // `who` writes to stdout the instant afterwards.
        let recorder = Recorder::default();
        let progress = Progress::spawn(Box::new(recorder.clone()));

        tokio::time::sleep(TICK * 5).await;
        progress.finish().await;

        let settled = recorder.writes();
        assert!(
            settled.len() > 1,
            "expected some repaints before finish, got {settled:?}"
        );
        assert_eq!(
            settled.last().map(String::as_str),
            Some("erase"),
            "finish must leave the line cleared: {settled:?}"
        );

        tokio::time::sleep(TICK * 4).await;
        assert_eq!(
            recorder.writes(),
            settled,
            "a repaint landed after finish returned"
        );
    }

    #[tokio::test]
    async fn a_run_shorter_than_one_tick_paints_nothing() {
        // `tokio::time::interval` fires its first tick immediately, so without
        // the grace tick a warm-cache `who` paints one frame and erases it —
        // a blink, on the command whose whole point is staying quiet when
        // there is nothing to wait for.
        let recorder = Recorder::default();
        let progress = Progress::spawn(Box::new(recorder.clone()));

        tokio::time::sleep(TICK / 2).await;
        progress.finish().await;

        assert_eq!(
            recorder.writes(),
            vec!["erase".to_owned()],
            "a command that finishes inside one tick must not blink a spinner"
        );
    }

    #[tokio::test]
    async fn a_note_clears_the_line_before_it_writes() {
        let recorder = Recorder::default();
        let progress = Progress::spawn(Box::new(recorder.clone()));

        tokio::time::sleep(TICK * 3).await;
        progress.note("pull request lookups timed out");
        let writes = recorder.writes();
        progress.finish().await;

        assert_eq!(
            writes.last().map(String::as_str),
            Some("erase"),
            "a note must erase the live line first: {writes:?}"
        );
    }

    #[tokio::test]
    async fn the_phase_the_caller_sets_is_the_phase_that_paints() {
        let recorder = Recorder::default();
        let progress = Progress::spawn(Box::new(recorder.clone()));

        progress.set(Phase::Resolving { done: 7, total: 11 });
        tokio::time::sleep(TICK * 3).await;
        progress.finish().await;

        assert!(
            recorder.writes().iter().any(|w| w.contains("7/11")),
            "{:?}",
            recorder.writes()
        );
    }
}
