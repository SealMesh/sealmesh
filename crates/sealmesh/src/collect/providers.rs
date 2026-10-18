//! The providers that ship with sealmesh.
//!
//! Each one wraps a reader that already existed and adds only what the trait
//! asks for: an identity, a priority, and — where it can prove one — a set of
//! live sessions. The readers themselves are unchanged, which is the point:
//! the trait is a seam, not a rewrite.

use std::path::PathBuf;

use jiff::SignedDuration;

use super::{
    Candidate, claude_sessions, declared,
    generic::{self, ProviderSpec},
    job,
    liveness::Liveness,
    provider::{Priority, Provider, ProviderId, ScanCtx, SessionRef},
    roots::Root,
    transcript,
};
use crate::wire::{TaskProgress, ToolId};

/// Every provider this build knows how to run, in no particular order —
/// precedence comes from `Priority`, not from position.
pub fn built_in(
    roots: Vec<Root>,
    declared_dir: Option<PathBuf>,
    declared_ttl: SignedDuration,
) -> Vec<Box<dyn Provider>> {
    with_configured(roots, declared_dir, declared_ttl, &[])
}

/// The built-in providers plus any described in config.
///
/// A spec that cannot produce a provider is skipped rather than fatal — but
/// `config::validate` has already refused the file by then, so in practice
/// this only drops a block that passed validation and still could not build.
pub fn with_configured(
    roots: Vec<Root>,
    declared_dir: Option<PathBuf>,
    declared_ttl: SignedDuration,
    configured: &[ProviderSpec],
) -> Vec<Box<dyn Provider>> {
    let mut providers: Vec<Box<dyn Provider>> = vec![
        Box::new(ClaudeJobs::new(roots.clone())),
        Box::new(ClaudeSessions::new(roots)),
    ];
    if let Some(dir) = declared_dir {
        providers.push(Box::new(Declared::new(dir, declared_ttl)));
    }
    for spec in configured {
        if let Some(provider) = generic::Generic::new(spec.clone()) {
            providers.push(Box::new(provider));
        }
    }
    providers
}

/// Point a candidate at the repo the agent is working in, not the directory it
/// was launched from.
///
/// `state.json` and a session file both record the launch directory. For anyone
/// whose repos sit side by side under one workspace, that is a checkout of
/// nothing: the resolver finds no repo, the gate answers `NoRepo`, and `who`
/// reports zero agents on a busy machine — which reads exactly like a machine
/// with nothing running.
///
/// The transcript knows better, because it records a cwd on every message. If
/// it names no checkout the launch directory stays, because it fails visibly in
/// `doctor` and a confidently wrong directory would publish work under the
/// wrong repo.
fn point_at_the_worktree(roots: &[Root], mut candidate: Candidate) -> Candidate {
    let Some(session_id) = candidate.session_id.as_deref() else {
        return candidate;
    };
    let found = roots.iter().find_map(|root| {
        let path = transcript::find(root, session_id)?;
        transcript::latest_checkout(&path)
    });
    if let Some(dir) = found {
        candidate.work_dir = dir;
    }
    candidate
}

/// Task counts for a session, read off whichever root answers first.
///
/// This used to run in the funnel for every candidate, which meant the funnel
/// held one vendor's directory layout in order to enrich rows that vendor did
/// not produce. It only ever found anything for a candidate with a Claude
/// session id, so moving it here changes nothing and takes `Root` out of the
/// funnel entirely.
fn tasks_for(roots: &[Root], session_id: Option<&str>) -> Option<TaskProgress> {
    roots
        .iter()
        .find_map(|root| job::task_progress(root, session_id))
        .and_then(|(completed, total)| TaskProgress::new(completed, total).ok())
}

pub struct ClaudeJobs {
    roots: Vec<Root>,
}

impl ClaudeJobs {
    pub fn new(roots: Vec<Root>) -> Self {
        Self { roots }
    }
}

impl Provider for ClaudeJobs {
    fn id(&self) -> ProviderId {
        ProviderId::CLAUDE_JOB
    }

    fn priority(&self) -> Priority {
        Priority::CLAUDE_JOB
    }

    fn scan(&self, _ctx: &ScanCtx<'_>) -> Vec<Candidate> {
        self.roots
            .iter()
            .flat_map(job::read_all)
            .map(|mut candidate| {
                candidate.tasks_hint = tasks_for(&self.roots, candidate.session_id.as_deref())
                    .or(candidate.tasks_hint);
                point_at_the_worktree(&self.roots, candidate)
            })
            .collect()
    }
}

pub struct ClaudeSessions {
    roots: Vec<Root>,
}

impl ClaudeSessions {
    pub fn new(roots: Vec<Root>) -> Self {
        Self { roots }
    }
}

impl Provider for ClaudeSessions {
    fn id(&self) -> ProviderId {
        ProviderId::CLAUDE_SESSION
    }

    fn priority(&self) -> Priority {
        Priority::CLAUDE_SESSION
    }

    fn live_sessions(&self, procs: &dyn Liveness) -> Vec<SessionRef> {
        self.roots
            .iter()
            .flat_map(|root| claude_sessions::read_all(root, procs))
            .filter_map(|candidate| {
                candidate.session_id.map(|id| SessionRef {
                    tool: ToolId::claude_code(),
                    id,
                })
            })
            .collect()
    }

    fn scan(&self, ctx: &ScanCtx<'_>) -> Vec<Candidate> {
        self.roots
            .iter()
            .flat_map(|root| claude_sessions::read_all(root, ctx.procs))
            .map(|mut candidate| {
                candidate.tasks_hint = tasks_for(&self.roots, candidate.session_id.as_deref())
                    .or(candidate.tasks_hint);
                point_at_the_worktree(&self.roots, candidate)
            })
            .collect()
    }
}

pub struct Declared {
    dir: PathBuf,
    ttl: SignedDuration,
}

impl Declared {
    pub fn new(dir: PathBuf, ttl: SignedDuration) -> Self {
        Self { dir, ttl }
    }
}

impl Provider for Declared {
    fn id(&self) -> ProviderId {
        ProviderId::DECLARED
    }

    fn priority(&self) -> Priority {
        Priority::DECLARED
    }

    fn scan(&self, ctx: &ScanCtx<'_>) -> Vec<Candidate> {
        let live = declared::LiveSessions(ctx.live_sessions);
        declared::read_all(&self.dir, self.ttl, ctx.now, &live)
            .into_iter()
            .filter_map(|read| read.outcome.ok())
            .collect()
    }
}
