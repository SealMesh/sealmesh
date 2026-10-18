//! The fan-out, and what one reader is allowed to see of it.
//!
//! The router cannot know which repos a machine is working in before it asks,
//! so every attached daemon is still asked — and still records who the router
//! says asked (rule 16). What is narrowed is the answer: an agent is kept only
//! when the reader's own token is confirmed to read its repo (rule 4).
//!
//! A machine is never dropped from the answer and never relabelled. One whose
//! work is all withheld goes out as `Answering` with no agents, which is
//! exactly what a machine with nothing running sends, so the two cannot be
//! told apart. Dropping the row instead would make a machine vanish between
//! two queries without ever going unreachable — a tell of its own — and would
//! make a repo renamed on GitHub look like a missing machine.
//!
//! Whether anything was withheld is one bit for the whole query, which the
//! caller sends as `ServerFrame::Partial`: never which host, which repo, or
//! how many (rule 25).

use std::{
    collections::{BTreeSet, VecDeque},
    time::{Duration, Instant},
};

use sealmesh_proto::{HostState, ProjectEntry, SecretToken};
use sealmesh_wire::{HostId, RepoSlug};

use crate::{
    access_cache::RepoKey,
    auth::{Directory, RepoAccess},
    check_budget::{CHECK_BURST, CheckBudgets, MAX_TRACKED_READERS},
    registry::Registry,
};

pub const FANOUT_TIMEOUT: Duration = Duration::from_secs(5);

/// Most distinct repos one request resolves against GitHub (rule 44). A
/// query's repos come from other machines' answers — up to `MAX_AGENTS` per
/// host, times however many hosts attach — and each can cost a GitHub call
/// made with the reader's token. A repo past this is withheld unresolved,
/// which is the fail-closed direction and reads as partial like any other.
pub const MAX_REPOS_PER_QUERY: usize = 32;

/// Wall-clock budget for resolving one request's repos, as one deadline shared
/// by every check. The GitHub client already times out each call, and up to
/// `MAX_CONCURRENT_CHECKS` run at once, but a request can queue more checks
/// than that, so without a total a GitHub outage would cost a client timeout
/// per batch. A check still running at the deadline is abandoned, and one not
/// yet started is never started; either way its repo is withheld.
pub const ACCESS_BUDGET: Duration = Duration::from_secs(10);

/// How many unverifiable repos one log line names, so a hostile answer
/// listing many repos cannot make one line unbounded.
const LOGGED_REPOS: usize = 8;

pub async fn ask_all(
    registry: &Registry,
    asked_by: &HostId,
    timeout: Duration,
) -> Vec<(HostId, HostState)> {
    let mut waiting = Vec::new();
    let mut out = Vec::new();

    for host in registry.known_hosts() {
        match registry.dispatch(&host, asked_by) {
            Some((ticket, rx)) => waiting.push((host, ticket, rx)),
            None => out.push((host, HostState::Unreachable {})),
        }
    }

    let deadline = Instant::now() + timeout;
    for (host, ticket, rx) in waiting {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let state = match tokio::time::timeout(remaining, rx).await {
            Ok(Ok(state)) => state,
            _ => {
                registry.forget(ticket);
                HostState::Unreachable {}
            }
        };
        out.push((host, state));
    }

    out.sort_by(|(a, _), (b, _)| a.cmp(b));
    out
}

/// What one reader is shown of one fan-out.
#[derive(Debug, PartialEq, Eq)]
pub struct Narrowed {
    pub reports: Vec<(HostId, HostState)>,
    /// Whether any agent was withheld, anywhere in this query.
    pub partial: bool,
}

/// Asks every attached daemon, then narrows the answers to what `token` can
/// read, charging each check GitHub has to answer to `asked_by`'s allowance in
/// `budgets`.
pub async fn ask_readable<D: Directory>(
    registry: &Registry,
    directory: &D,
    budgets: &CheckBudgets,
    token: &SecretToken,
    asked_by: &HostId,
    timeout: Duration,
) -> Narrowed {
    let reports = ask_all(registry, asked_by, timeout).await;
    narrow(directory, budgets, token, asked_by, reports, ACCESS_BUDGET).await
}

/// The router's project mapping, cut down to the repos `token` can read. A
/// project with no readable repo left is dropped whole rather than sent empty,
/// because an empty entry still says the project exists (rule 25).
pub async fn readable_projects<D: Directory>(
    directory: &D,
    token: &SecretToken,
    asked_by: &HostId,
    projects: Vec<ProjectEntry>,
) -> Vec<ProjectEntry> {
    filter_projects(directory, token, asked_by, projects, ACCESS_BUDGET).await
}

/// Every answering host's agents, cut down to the repos `token` is confirmed
/// to read.
///
/// A row is kept only when its repo resolved `Readable`. Whatever else
/// happened to it — GitHub said no, GitHub could not be asked, the request's
/// deadline passed, the reader's check allowance was spent, or the repo came
/// past `MAX_REPOS_PER_QUERY` so no check was made at all — it is withheld,
/// and any withheld row makes the whole answer partial. That last case is
/// what keeps a host late in the round-robin, past the thirty-second
/// answering host, from reading as a machine running nothing.
async fn narrow<D: Directory>(
    directory: &D,
    budgets: &CheckBudgets,
    token: &SecretToken,
    asked_by: &HostId,
    mut reports: Vec<(HostId, HostState)>,
    budget: Duration,
) -> Narrowed {
    let wanted = fair_order(reports.iter().filter_map(|(_, state)| match state {
        HostState::Answering { snapshot } => Some(snapshot.agents.iter().map(|agent| &agent.repo)),
        HostState::NotSharing {} | HostState::Unreachable {} | HostState::Throttled {} => None,
    }));

    let readable = readable_repos(
        directory,
        Payer::Reader(budgets),
        token,
        asked_by,
        wanted,
        budget,
    )
    .await;

    let mut partial = false;
    for (_, state) in &mut reports {
        if let HostState::Answering { snapshot } = state {
            let before = snapshot.agents.len();
            snapshot
                .agents
                .retain(|agent| readable.contains(&RepoKey::of(&agent.repo)));
            partial |= snapshot.agents.len() != before;
        }
    }

    Narrowed { reports, partial }
}

async fn filter_projects<D: Directory>(
    directory: &D,
    token: &SecretToken,
    asked_by: &HostId,
    projects: Vec<ProjectEntry>,
    budget: Duration,
) -> Vec<ProjectEntry> {
    let wanted = fair_order(projects.iter().map(|project| project.repos.iter()));

    let readable =
        readable_repos(directory, Payer::RouterMap, token, asked_by, wanted, budget).await;

    projects
        .into_iter()
        .filter_map(|mut project| {
            project
                .repos
                .retain(|repo| readable.contains(&RepoKey::of(repo)));
            (!project.repos.is_empty()).then_some(project)
        })
        .collect()
}

/// Distinct repos in the order they will be checked: one from each source in
/// turn, each source keeping its own order.
///
/// The cap is per request, so this order decides whose work gets resolved
/// when there is more than the cap allows. A sorted set would let one machine
/// that lists many repos sorting early push every other machine's work past
/// the cap — withheld, and reading as nothing running. Taking turns means
/// every source has its first repos checked before any source has its
/// thirty-third. A snapshot lists its most recently updated agents first, so
/// within a machine the freshest work goes first too.
///
/// Distinct means distinct on GitHub, where names are case-insensitive: the
/// first spelling seen is the one checked, and its answer covers every other.
fn fair_order<'a, S, I>(sources: S) -> Vec<RepoSlug>
where
    S: IntoIterator<Item = I>,
    I: IntoIterator<Item = &'a RepoSlug>,
{
    let mut queues: Vec<VecDeque<&'a RepoSlug>> = sources
        .into_iter()
        .map(|source| source.into_iter().collect())
        .collect();
    let mut seen: BTreeSet<RepoKey> = BTreeSet::new();
    let mut order = Vec::new();

    loop {
        let mut took_any = false;
        for queue in &mut queues {
            // A repo an earlier source already queued does not use up this
            // source's turn; keep going until this source adds one of its own.
            while let Some(repo) = queue.pop_front() {
                took_any = true;
                if seen.insert(RepoKey::of(repo)) {
                    order.push(repo.clone());
                    break;
                }
            }
        }
        if !took_any {
            return order;
        }
    }
}

/// Most access checks one request has in flight at once.
///
/// Checks used to run one after another, so a request's latency was the sum
/// of its GitHub round trips. `who` waits only a few seconds for the Projects
/// reply, and on a cold cache a slow GitHub made even a full-access reader
/// fall back to ungrouped output, silently. Running a few at once makes the
/// latency the slowest round trip instead. The bound keeps one request from
/// opening a burst of simultaneous calls on the reader's token.
pub const MAX_CONCURRENT_CHECKS: usize = 8;

/// One repo's outcome, as the tally below needs it.
enum Checked {
    Readable,
    Refused,
    Unverifiable,
    /// Not checked at all: the budget was already spent when its turn came.
    Unresolved,
    /// Not checked at all: GitHub would have had to answer, and the reader's
    /// allowance of such checks was spent.
    OverBudget,
}

impl From<RepoAccess> for Checked {
    fn from(access: RepoAccess) -> Self {
        match access {
            RepoAccess::Readable => Self::Readable,
            RepoAccess::NotReadable => Self::Refused,
            RepoAccess::Unverifiable => Self::Unverifiable,
        }
    }
}

/// Where the repos being resolved came from, which decides who pays for a
/// check the cache cannot answer and whether a log line may name them.
#[derive(Clone, Copy)]
enum Payer<'a> {
    /// Named by other machines' answers, so hostile input: each check GitHub
    /// has to answer is charged to the reader's allowance first, and a repo is
    /// never named in a log line (rule 14).
    Reader(&'a CheckBudgets),
    /// The router's own project map, bounded by its configuration and chosen
    /// by its operator: not charged, and safe to name.
    RouterMap,
}

/// The log line for `count` unverifiable repos, pulled out of [`readable_repos`]
/// so its content can be pinned by a test rather than only read off a
/// terminal.
///
/// Only [`Payer::RouterMap`] ever names a repo. A repo named by
/// [`Payer::Reader`] came out of a daemon's answer — peer bytes — and a daemon
/// does not get to write into the router's log (rule 14), so that branch never
/// reaches into `unverifiable` at all.
fn unverifiable_log_line(payer: Payer<'_>, asked_by: &HostId, unverifiable: &[RepoSlug]) -> String {
    let named: Vec<&str> = match payer {
        Payer::RouterMap => unverifiable
            .iter()
            .take(LOGGED_REPOS)
            .map(RepoSlug::as_str)
            .collect(),
        Payer::Reader(_) => Vec::new(),
    };
    let listed = if named.is_empty() {
        String::new()
    } else {
        let more = if unverifiable.len() > named.len() {
            ", …"
        } else {
            ""
        };
        format!(" ({}{more})", named.join(", "))
    };
    format!(
        "sealmesh-server: could not verify {asked_by}'s read access to {} repo(s){listed}; \
         withheld as unreadable. A repo renamed on GitHub stays unverifiable until its \
         checkout uses the new name.",
        unverifiable.len(),
    )
}

/// The repos, out of `wanted`, that `token` is confirmed to read, as
/// case-folded keys.
///
/// At most `MAX_REPOS_PER_QUERY` of them are checked, taken from the front of
/// `wanted` — so the caller's order decides whose repos are checked — with up
/// to `MAX_CONCURRENT_CHECKS` in flight at once and all of them inside
/// `budget`. For [`Payer::Reader`], a check the access cache can already
/// answer is free, and any other is made only if the reader's allowance
/// covers it.
///
/// Only `Readable` counts. `NotReadable`, `Unverifiable`, a call that did not
/// finish inside `budget`, a check the allowance refused, and a repo past
/// `MAX_REPOS_PER_QUERY` are all left out, and nothing the caller receives
/// tells one from another. The server's own log does, for the cases that are
/// not a clean answer from GitHub (rule 28) — never with the token or
/// GitHub's response bytes.
async fn readable_repos<D: Directory>(
    directory: &D,
    payer: Payer<'_>,
    token: &SecretToken,
    asked_by: &HostId,
    wanted: Vec<RepoSlug>,
    budget: Duration,
) -> BTreeSet<RepoKey> {
    // A deadline that cannot be represented is treated as already passed:
    // nothing is checked and everything is withheld, the fail-closed side.
    let now = tokio::time::Instant::now();
    let deadline = now.checked_add(budget).unwrap_or(now);

    let mut unresolved = wanted.len().saturating_sub(MAX_REPOS_PER_QUERY);
    let mut checks = wanted
        .into_iter()
        .take(MAX_REPOS_PER_QUERY)
        .map(|repo| async move {
            if tokio::time::Instant::now() >= deadline {
                return (repo, Checked::Unresolved);
            }
            if let Payer::Reader(budgets) = payer {
                // Charged here, when this check has its slot and is about to
                // be made, never ahead of the call it pays for. A cache entry
                // that lands between this lookup and the call below makes the
                // charge an over-count, the safe direction.
                if let Some(known) = directory.known_access(token, &repo) {
                    return (repo, Checked::from(known));
                }
                if !budgets.try_charge(asked_by) {
                    return (repo, Checked::OverBudget);
                }
            }
            let checked =
                match tokio::time::timeout_at(deadline, directory.can_read(token, &repo)).await {
                    Ok(access) => Checked::from(access),
                    Err(_) => Checked::Unverifiable,
                };
            (repo, checked)
        });

    // A bounded join, polled in place rather than spawned: the checks borrow
    // the directory and the token, and nothing here holds a lock of its own.
    let mut in_flight = Vec::with_capacity(MAX_CONCURRENT_CHECKS);
    let mut finished: Vec<(RepoSlug, Checked)> = Vec::new();
    std::future::poll_fn(|cx| {
        loop {
            while in_flight.len() < MAX_CONCURRENT_CHECKS {
                match checks.next() {
                    Some(check) => in_flight.push(Box::pin(check)),
                    None => break,
                }
            }
            // Empty straight after a refill means there is nothing left to
            // start. Checked before polling, not after: a batch that all
            // finishes at once also leaves this empty, with more still queued.
            if in_flight.is_empty() {
                return std::task::Poll::Ready(());
            }
            let finished_before = finished.len();
            in_flight.retain_mut(|check| match check.as_mut().poll(cx) {
                std::task::Poll::Ready(outcome) => {
                    finished.push(outcome);
                    false
                }
                std::task::Poll::Pending => true,
            });
            // Nothing finished, so every check in flight has registered its
            // waker; a finished one freed a slot, so go round and fill it.
            if finished.len() == finished_before {
                return std::task::Poll::Pending;
            }
        }
    })
    .await;

    let mut readable = BTreeSet::new();
    let mut unverifiable: Vec<RepoSlug> = Vec::new();
    let mut refused = 0usize;
    let mut over_budget = 0usize;
    for (repo, checked) in finished {
        match checked {
            Checked::Readable => {
                readable.insert(RepoKey::of(&repo));
            }
            Checked::Refused => refused = refused.saturating_add(1),
            Checked::Unverifiable => unverifiable.push(repo),
            Checked::Unresolved => unresolved = unresolved.saturating_add(1),
            Checked::OverBudget => over_budget = over_budget.saturating_add(1),
        }
    }
    // Completion order is arbitrary; keep the log line stable.
    unverifiable.sort();

    // Rule 28 covers authorization refusals too. A count, once per request,
    // and no repo names: a clean no from GitHub needs no diagnosis, only a
    // trace that narrowing happened.
    if refused > 0 {
        eprintln!(
            "sealmesh-server: {asked_by} cannot read {refused} repo(s) in this request; \
             their work was withheld"
        );
    }
    if !unverifiable.is_empty() {
        eprintln!("{}", unverifiable_log_line(payer, asked_by, &unverifiable));
    }
    if over_budget > 0 {
        let per_hour = match payer {
            Payer::Reader(budgets) => budgets.per_hour().get(),
            Payer::RouterMap => 0,
        };
        // Both refusals `try_charge` can make are named, because the log is
        // the only place an operator can tell them apart.
        eprintln!(
            "sealmesh-server: withheld {over_budget} repo(s) from {asked_by}'s request \
             unchecked: either its allowance of {CHECK_BURST} uncached access checks, \
             refilling at {per_hour} an hour, is spent, or the router already holds \
             {MAX_TRACKED_READERS} readers' allowances and none has refilled"
        );
    }
    if unresolved > 0 {
        eprintln!(
            "sealmesh-server: withheld {unresolved} repo(s) from {asked_by}'s request \
             unresolved, past the limit of {MAX_REPOS_PER_QUERY} repos or the {}ms budget",
            budget.as_millis()
        );
    }

    readable
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::sync::atomic::{AtomicUsize, Ordering};

    use std::sync::Arc;

    use super::*;
    use crate::{access_cache::CachingDirectory, auth::Denied, check_budget::ChecksPerHour};
    use sealmesh_proto::ServerFrame;
    use sealmesh_wire::{MAX_AGENTS, Snapshot};

    /// [`super::narrow`] with a fresh reader allowance on every call, for the
    /// tests about narrowing rather than about the allowance. One burst covers
    /// any single query: it is twice `MAX_REPOS_PER_QUERY`.
    async fn narrow<D: Directory>(
        directory: &D,
        token: &SecretToken,
        asked_by: &HostId,
        reports: Vec<(HostId, HostState)>,
        budget: Duration,
    ) -> Narrowed {
        super::narrow(
            directory,
            &CheckBudgets::new(ChecksPerHour::MIN),
            token,
            asked_by,
            reports,
            budget,
        )
        .await
    }

    fn host(name: &str) -> HostId {
        HostId::try_from(name).unwrap()
    }

    fn repo(slug: &str) -> RepoSlug {
        RepoSlug::try_from(slug).unwrap()
    }

    fn token(value: &str) -> SecretToken {
        SecretToken::new(value.to_owned())
    }

    /// An `Answering` state for `snapshot_host`, one agent per repo, with a
    /// fixed collection time so two answers differ only in what they carry.
    fn answering(snapshot_host: &str, repos: &[&str]) -> HostState {
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
        HostState::Answering {
            snapshot: Box::new(Snapshot::from_json(json.as_bytes()).unwrap()),
        }
    }

    fn agent_repos(state: &HostState) -> Vec<String> {
        match state {
            HostState::Answering { snapshot } => snapshot
                .agents
                .iter()
                .map(|a| a.repo.as_str().to_owned())
                .collect(),
            other => panic!("expected an answer, got {other:?}"),
        }
    }

    /// Grants by exact slug, and only to the one token it was built for.
    struct Access {
        token: &'static str,
        readable: &'static [&'static str],
        unverifiable: &'static [&'static str],
        calls: AtomicUsize,
    }

    impl Access {
        fn new(readable: &'static [&'static str]) -> Self {
            Self {
                token: "ghp_reader",
                readable,
                unverifiable: &[],
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl Directory for Access {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Ok("reader".to_owned())
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(true)
        }
        async fn can_read(&self, token: &SecretToken, repo: &RepoSlug) -> RepoAccess {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if token.expose() != self.token {
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

    /// Grants everything, and counts.
    struct Everything {
        calls: AtomicUsize,
    }

    impl Directory for Everything {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Ok("reader".to_owned())
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(true)
        }
        async fn can_read(&self, _token: &SecretToken, _repo: &RepoSlug) -> RepoAccess {
            self.calls.fetch_add(1, Ordering::SeqCst);
            RepoAccess::Readable
        }
    }

    /// GitHub never answering at all.
    struct Hangs;

    impl Directory for Hangs {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Ok("reader".to_owned())
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(true)
        }
        async fn can_read(&self, _token: &SecretToken, _repo: &RepoSlug) -> RepoAccess {
            std::future::pending::<RepoAccess>().await
        }
    }

    #[tokio::test]
    async fn a_host_with_no_daemon_reports_unreachable_rather_than_vanishing() {
        let registry = Registry::new();
        let (_rx, guard) = registry.attach(host("sandbank"));
        drop(guard);

        let report = ask_all(&registry, &host("reader"), FANOUT_TIMEOUT).await;
        assert_eq!(report, vec![(host("sandbank"), HostState::Unreachable {})]);
    }

    #[tokio::test]
    async fn an_answering_daemon_is_reported_as_it_answered() {
        let registry = Registry::new();
        let (mut rx, _guard) = registry.attach(host("sandbank"));

        let answering = async {
            if let Some(ServerFrame::Query { id, .. }) = rx.recv().await {
                registry.resolve(id, HostState::NotSharing {});
            }
        };
        let reader = host("reader");
        let (report, ()) = tokio::join!(ask_all(&registry, &reader, FANOUT_TIMEOUT), answering);

        assert_eq!(report, vec![(host("sandbank"), HostState::NotSharing {})]);
    }

    #[tokio::test]
    async fn a_throttled_daemon_is_relayed_as_throttled_and_passes_narrowing_untouched() {
        let registry = Registry::new();
        let (mut rx, _guard) = registry.attach(host("sandbank"));

        let refusing = async {
            if let Some(ServerFrame::Query { id, .. }) = rx.recv().await {
                registry.resolve(id, HostState::Throttled {});
            }
        };
        let reader = host("reader");
        let (report, ()) = tokio::join!(ask_all(&registry, &reader, FANOUT_TIMEOUT), refusing);
        assert_eq!(report, vec![(host("sandbank"), HostState::Throttled {})]);

        let narrowed = narrow(
            &Access::new(&[]),
            &token("ghp_reader"),
            &reader,
            report,
            ACCESS_BUDGET,
        )
        .await;
        assert_eq!(
            narrowed,
            Narrowed {
                reports: vec![(host("sandbank"), HostState::Throttled {})],
                partial: false,
            }
        );
    }

    #[tokio::test]
    async fn a_silent_daemon_times_out_without_holding_the_others() {
        let registry = Registry::new();
        let (_silent, _g1) = registry.attach(host("quiet"));
        let (mut prompt, _g2) = registry.attach(host("sandbank"));

        let answering = async {
            if let Some(ServerFrame::Query { id, .. }) = prompt.recv().await {
                registry.resolve(id, HostState::NotSharing {});
            }
        };

        let started = Instant::now();
        let timeout = Duration::from_millis(200);
        let reader = host("reader");
        let (report, ()) = tokio::join!(ask_all(&registry, &reader, timeout), answering);

        assert!(
            started.elapsed() < timeout * 3,
            "one slow host must not multiply the wait: took {:?}",
            started.elapsed()
        );
        assert_eq!(
            report,
            vec![
                (host("quiet"), HostState::Unreachable {}),
                (host("sandbank"), HostState::NotSharing {}),
            ]
        );
    }

    #[tokio::test]
    async fn a_timed_out_query_does_not_leak() {
        let registry = Registry::new();
        let (_silent, _guard) = registry.attach(host("quiet"));

        let report = ask_all(&registry, &host("reader"), Duration::from_millis(50)).await;
        assert_eq!(report, vec![(host("quiet"), HostState::Unreachable {})]);

        let report = ask_all(&registry, &host("reader"), Duration::from_millis(50)).await;
        assert_eq!(report.len(), 1, "a second sweep still reports the host");
    }

    #[tokio::test]
    async fn work_in_a_repo_the_reader_cannot_read_is_withheld_and_the_answer_says_partial() {
        let directory = Access::new(&["example-org/sealmesh"]);
        let reports = vec![(
            host("sandbank"),
            answering("sandbank", &["example-org/sealmesh", "example-org/private"]),
        )];

        let narrowed = narrow(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            reports,
            ACCESS_BUDGET,
        )
        .await;

        assert!(narrowed.partial);
        let (_, state) = narrowed.reports.first().unwrap();
        assert_eq!(agent_repos(state), vec!["example-org/sealmesh"]);
    }

    #[tokio::test]
    async fn a_machine_whose_work_is_all_withheld_is_identical_to_one_running_nothing() {
        let directory = Access::new(&[]);
        let hidden = narrow(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            vec![(
                host("sandbank"),
                answering("sandbank", &["example-org/private"]),
            )],
            ACCESS_BUDGET,
        )
        .await;
        let idle = narrow(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            vec![(host("sandbank"), answering("sandbank", &[]))],
            ACCESS_BUDGET,
        )
        .await;

        // The rows are the same bytes on the wire. The only difference between
        // the two queries is the single query-wide partial bit.
        let frame = |reports: &[(HostId, HostState)]| {
            let (host, state) = reports.first().unwrap().clone();
            serde_json::to_string(&ServerFrame::Report {
                id: sealmesh_proto::QueryId(1),
                host,
                state,
            })
            .unwrap()
        };
        assert_eq!(frame(&hidden.reports), frame(&idle.reports));
        assert!(hidden.partial);
        assert!(!idle.partial);
    }

    #[tokio::test]
    async fn an_unverifiable_repo_is_withheld_exactly_like_an_unreadable_one() {
        let outage = Access {
            unverifiable: &["example-org/private"],
            ..Access::new(&["example-org/sealmesh"])
        };
        let refusal = Access::new(&["example-org/sealmesh"]);
        let reports = || {
            vec![(
                host("sandbank"),
                answering("sandbank", &["example-org/sealmesh", "example-org/private"]),
            )]
        };

        let from_outage = narrow(
            &outage,
            &token("ghp_reader"),
            &host("reader"),
            reports(),
            ACCESS_BUDGET,
        )
        .await;
        let from_refusal = narrow(
            &refusal,
            &token("ghp_reader"),
            &host("reader"),
            reports(),
            ACCESS_BUDGET,
        )
        .await;

        assert_eq!(from_outage, from_refusal, "an outage failed open");
    }

    #[tokio::test]
    async fn an_answer_with_nothing_withheld_is_not_partial() {
        let directory = Access::new(&["example-org/sealmesh", "example-org/example-repo"]);
        let reports = vec![
            (
                host("sandbank"),
                answering("sandbank", &["example-org/sealmesh"]),
            ),
            (
                host("shelter"),
                answering("shelter", &["example-org/example-repo"]),
            ),
        ];

        let narrowed = narrow(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            reports.clone(),
            ACCESS_BUDGET,
        )
        .await;

        assert_eq!(
            narrowed,
            Narrowed {
                reports,
                partial: false
            }
        );
    }

    #[tokio::test]
    async fn machines_that_did_not_answer_pass_through_untouched_and_are_never_dropped() {
        let directory = Access::new(&[]);
        let reports = vec![
            (host("prism"), HostState::Unreachable {}),
            (
                host("sandbank"),
                answering("sandbank", &["example-org/private"]),
            ),
            (host("shelter"), HostState::NotSharing {}),
        ];

        let narrowed = narrow(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            reports,
            ACCESS_BUDGET,
        )
        .await;

        let hosts: Vec<&str> = narrowed.reports.iter().map(|(h, _)| h.as_str()).collect();
        assert_eq!(hosts, vec!["prism", "sandbank", "shelter"]);
        assert_eq!(
            narrowed.reports.first().map(|(_, s)| s),
            Some(&HostState::Unreachable {})
        );
        assert_eq!(
            narrowed.reports.last().map(|(_, s)| s),
            Some(&HostState::NotSharing {})
        );
    }

    #[tokio::test]
    async fn narrowing_never_relabels_the_host_a_snapshot_claims() {
        // Rule 21 lives in the reader, and it needs the snapshot's own `host`
        // intact to catch a router that attributes an answer to the wrong
        // machine. Narrowing touches the agents and nothing else.
        let directory = Access::new(&["example-org/sealmesh"]);
        let reports = vec![(
            host("shelter"),
            answering("sandbank", &["example-org/sealmesh"]),
        )];

        let narrowed = narrow(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            reports,
            ACCESS_BUDGET,
        )
        .await;

        let (attributed, state) = narrowed.reports.first().unwrap();
        assert_eq!(attributed.as_str(), "shelter");
        let HostState::Answering { snapshot } = state else {
            panic!("{state:?}");
        };
        assert_eq!(snapshot.host.as_str(), "sandbank");
    }

    #[tokio::test]
    async fn access_is_resolved_with_the_readers_own_token_and_no_other() {
        let directory = Access::new(&["example-org/sealmesh"]);
        let reports = || {
            vec![(
                host("sandbank"),
                answering("sandbank", &["example-org/sealmesh"]),
            )]
        };

        let own = narrow(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            reports(),
            ACCESS_BUDGET,
        )
        .await;
        let someone_else = narrow(
            &directory,
            &token("ghp_other"),
            &host("reader"),
            reports(),
            ACCESS_BUDGET,
        )
        .await;

        assert!(!own.partial);
        assert!(someone_else.partial);
    }

    #[tokio::test]
    async fn a_repo_is_resolved_once_per_query_however_many_agents_share_it() {
        let directory = Access::new(&["example-org/sealmesh"]);
        let reports = vec![
            (
                host("sandbank"),
                answering(
                    "sandbank",
                    &["example-org/sealmesh", "example-org/sealmesh"],
                ),
            ),
            (
                host("shelter"),
                answering("shelter", &["example-org/sealmesh"]),
            ),
        ];

        narrow(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            reports,
            ACCESS_BUDGET,
        )
        .await;

        assert_eq!(directory.calls(), 1);
    }

    #[tokio::test]
    async fn a_query_with_no_agents_resolves_nothing() {
        let directory = Access::new(&[]);
        let narrowed = narrow(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            vec![
                (host("sandbank"), answering("sandbank", &[])),
                (host("shelter"), HostState::NotSharing {}),
            ],
            ACCESS_BUDGET,
        )
        .await;

        assert_eq!(directory.calls(), 0);
        assert!(!narrowed.partial);
    }

    #[tokio::test]
    async fn no_more_than_the_cap_of_repos_is_resolved_per_query() {
        let directory = Everything {
            calls: AtomicUsize::new(0),
        };
        let over = MAX_REPOS_PER_QUERY + 8;
        let names: Vec<String> = (0..over).map(|i| format!("example-org/r{i:03}")).collect();
        let reports: Vec<(HostId, HostState)> = names
            .chunks(40)
            .enumerate()
            .map(|(i, chunk)| {
                let name = format!("host-{i}");
                let repos: Vec<&str> = chunk.iter().map(String::as_str).collect();
                (host(&name), answering(&name, &repos))
            })
            .collect();

        let narrowed = narrow(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            reports,
            ACCESS_BUDGET,
        )
        .await;

        assert_eq!(directory.calls.load(Ordering::SeqCst), MAX_REPOS_PER_QUERY);
        assert!(
            narrowed.partial,
            "work past the cap is withheld, and says so"
        );
        let kept: usize = narrowed
            .reports
            .iter()
            .map(|(_, state)| agent_repos(state).len())
            .sum();
        assert_eq!(kept, MAX_REPOS_PER_QUERY);
    }

    #[tokio::test]
    async fn one_machine_listing_many_repos_cannot_push_another_machines_work_past_the_cap() {
        // Checked in sorted order, these repos would all be resolved before
        // `zzz-software/real`, the cap would be spent, and sandbank's real work
        // would be withheld — reading as a machine running nothing.
        let directory = Everything {
            calls: AtomicUsize::new(0),
        };
        let crowd: Vec<String> = (0..MAX_REPOS_PER_QUERY + 8)
            .map(|i| format!("aaa-software/r{i:03}"))
            .collect();
        let crowd: Vec<&str> = crowd.iter().map(String::as_str).collect();
        let reports = vec![
            (host("crowd"), answering("crowd", &crowd)),
            (
                host("sandbank"),
                answering("sandbank", &["zzz-software/real"]),
            ),
        ];

        let narrowed = narrow(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            reports,
            ACCESS_BUDGET,
        )
        .await;

        let (_, sandbank) = narrowed
            .reports
            .iter()
            .find(|(h, _)| h.as_str() == "sandbank")
            .unwrap();
        assert_eq!(agent_repos(sandbank), vec!["zzz-software/real"]);
        assert_eq!(directory.calls.load(Ordering::SeqCst), MAX_REPOS_PER_QUERY);
        assert!(narrowed.partial, "the crowd's overflow is still withheld");
    }

    /// Grants everything, counting through a shared counter so a cache
    /// wrapped around it can be counted from outside.
    struct Counted {
        calls: Arc<AtomicUsize>,
    }

    impl Directory for Counted {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Ok("reader".to_owned())
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(true)
        }
        async fn can_read(&self, _token: &SecretToken, _repo: &RepoSlug) -> RepoAccess {
            self.calls.fetch_add(1, Ordering::SeqCst);
            RepoAccess::Readable
        }
    }

    /// `count` repo names nobody has asked about before: a new owner per
    /// daemon per round, so no cache and no earlier query has seen them.
    fn fresh(round: usize, daemon: &str, count: usize) -> Vec<String> {
        (0..count)
            .map(|i| format!("{daemon}-{round}/fake{i:02}"))
            .collect()
    }

    fn answering_all(snapshot_host: &str, repos: &[String]) -> HostState {
        let repos: Vec<&str> = repos.iter().map(String::as_str).collect();
        answering(snapshot_host, &repos)
    }

    fn burst() -> usize {
        usize::try_from(CHECK_BURST).unwrap()
    }

    #[tokio::test]
    async fn hostile_daemons_naming_fresh_repos_in_every_answer_cannot_spend_the_reader_past_its_allowance()
     {
        // Before the allowance, these ten queries cost the reader's token ten
        // times MAX_REPOS_PER_QUERY GitHub calls — 320 — and an eleventh would
        // have cost 32 more. Two daemons, so an allowance per daemon would
        // show up as twice the burst.
        let directory = Everything {
            calls: AtomicUsize::new(0),
        };
        let budgets = CheckBudgets::new(ChecksPerHour::MIN);
        let reader = host("reader");
        let reader_token = token("ghp_reader");

        for round in 0..10 {
            let reports: Vec<(HostId, HostState)> = ["magenta", "trudy"]
                .iter()
                .map(|daemon| {
                    (
                        host(daemon),
                        answering_all(daemon, &fresh(round, daemon, MAX_AGENTS)),
                    )
                })
                .collect();
            let before = directory.calls.load(Ordering::SeqCst);
            let narrowed = super::narrow(
                &directory,
                &budgets,
                &reader_token,
                &reader,
                reports,
                ACCESS_BUDGET,
            )
            .await;
            let spent = directory.calls.load(Ordering::SeqCst) - before;

            assert!(narrowed.partial, "round {round} withheld rows silently");
            let kept: usize = narrowed
                .reports
                .iter()
                .map(|(_, state)| agent_repos(state).len())
                .sum();
            assert_eq!(kept, spent, "round {round}: a row was shown with no check");
        }

        assert_eq!(directory.calls.load(Ordering::SeqCst), burst());
    }

    #[tokio::test]
    async fn once_the_allowance_is_spent_fresh_repos_are_withheld_unchecked_and_the_answer_is_partial()
     {
        // Eight repos a query is well under the cap, so the only thing that
        // can make these answers partial is the allowance.
        let directory = Everything {
            calls: AtomicUsize::new(0),
        };
        let budgets = CheckBudgets::new(ChecksPerHour::MIN);
        let reader = host("reader");
        let reader_token = token("ghp_reader");
        let per_query = 8;
        let covered = burst() / per_query;

        for round in 0..covered + 4 {
            let narrowed = super::narrow(
                &directory,
                &budgets,
                &reader_token,
                &reader,
                vec![(
                    host("magenta"),
                    answering_all("magenta", &fresh(round, "magenta", per_query)),
                )],
                ACCESS_BUDGET,
            )
            .await;
            let (_, state) = narrowed.reports.first().unwrap();
            if round < covered {
                assert!(!narrowed.partial, "round {round}");
                assert_eq!(agent_repos(state).len(), per_query, "round {round}");
            } else {
                assert!(
                    narrowed.partial,
                    "round {round}: refused checks without Partial"
                );
                assert!(
                    agent_repos(state).is_empty(),
                    "round {round}: an unchecked repo was shown: {:?}",
                    agent_repos(state)
                );
            }
        }

        assert_eq!(directory.calls.load(Ordering::SeqCst), burst());
    }

    #[tokio::test]
    async fn a_cached_answer_costs_the_reader_nothing_and_is_still_served_once_the_allowance_is_spent()
     {
        let calls = Arc::new(AtomicUsize::new(0));
        let directory = CachingDirectory::new(
            Counted {
                calls: Arc::clone(&calls),
            },
            Duration::from_secs(60),
        );
        let budgets = CheckBudgets::new(ChecksPerHour::MIN);
        let reader = host("reader");
        let reader_token = token("ghp_reader");
        let real = || {
            (
                host("sandbank"),
                answering("sandbank", &["example-org/sealmesh"]),
            )
        };

        for _ in 0..200 {
            let narrowed = super::narrow(
                &directory,
                &budgets,
                &reader_token,
                &reader,
                vec![real()],
                ACCESS_BUDGET,
            )
            .await;
            assert!(!narrowed.partial, "a cached repo was charged and refused");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Only that first check was charged, so the rest of the burst is
        // still there for fresh repos.
        for round in 0..3 {
            super::narrow(
                &directory,
                &budgets,
                &reader_token,
                &reader,
                vec![(
                    host("magenta"),
                    answering_all("magenta", &fresh(round, "magenta", MAX_REPOS_PER_QUERY)),
                )],
                ACCESS_BUDGET,
            )
            .await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), burst());

        let narrowed = super::narrow(
            &directory,
            &budgets,
            &reader_token,
            &reader,
            vec![
                real(),
                (host("magenta"), answering("magenta", &["magenta-9/fake00"])),
            ],
            ACCESS_BUDGET,
        )
        .await;
        assert!(narrowed.partial);
        let (_, sandbank) = narrowed.reports.first().unwrap();
        let (_, magenta) = narrowed.reports.last().unwrap();
        assert_eq!(agent_repos(sandbank), vec!["example-org/sealmesh"]);
        assert!(agent_repos(magenta).is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), burst());
    }

    #[tokio::test]
    async fn one_readers_spent_allowance_does_not_withhold_another_readers_answer() {
        let directory = Everything {
            calls: AtomicUsize::new(0),
        };
        let budgets = CheckBudgets::new(ChecksPerHour::MIN);
        let ask = |reader: &'static str, round: usize, count: usize| {
            let reports = vec![(
                host("magenta"),
                answering_all("magenta", &fresh(round, "magenta", count)),
            )];
            let budgets = &budgets;
            let directory = &directory;
            async move {
                super::narrow(
                    directory,
                    budgets,
                    &token(&format!("ghp_{reader}")),
                    &host(reader),
                    reports,
                    ACCESS_BUDGET,
                )
                .await
            }
        };

        for round in 0..2 {
            ask("atlas", round, MAX_REPOS_PER_QUERY).await;
        }
        let atlas = ask("atlas", 2, 8).await;
        assert!(atlas.partial, "atlas's allowance was not spent");

        let bay = ask("bay", 3, 8).await;
        assert!(!bay.partial, "atlas's spending withheld bay's answer");
        let (_, state) = bay.reports.first().unwrap();
        assert_eq!(agent_repos(state).len(), 8);
    }

    #[tokio::test]
    async fn the_allowance_follows_the_readers_host_not_the_token_it_presents() {
        // Rule 12: the key is the host the credential resolved to. A reader
        // presenting a fresh token each time is still one allowance, and a
        // different host presenting the same token is another.
        let directory = Everything {
            calls: AtomicUsize::new(0),
        };
        let budgets = CheckBudgets::new(ChecksPerHour::MIN);
        let ask = |reader: &'static str, secret: &'static str, round: usize, count: usize| {
            let reports = vec![(
                host("magenta"),
                answering_all("magenta", &fresh(round, "magenta", count)),
            )];
            let budgets = &budgets;
            let directory = &directory;
            async move {
                super::narrow(
                    directory,
                    budgets,
                    &token(secret),
                    &host(reader),
                    reports,
                    ACCESS_BUDGET,
                )
                .await
            }
        };

        ask("atlas", "ghp_first", 0, MAX_REPOS_PER_QUERY).await;
        ask("atlas", "ghp_second", 1, MAX_REPOS_PER_QUERY).await;
        let rotated = ask("atlas", "ghp_third", 2, 8).await;
        assert!(
            rotated.partial,
            "a new token bought atlas a fresh allowance"
        );

        let bay = ask("bay", "ghp_first", 3, 8).await;
        assert!(
            !bay.partial,
            "bay was charged to the allowance of atlas's token"
        );
    }

    #[tokio::test]
    async fn case_variants_of_one_repo_across_machines_cost_one_check_and_each_row_keeps_its_spelling()
     {
        let directory = Everything {
            calls: AtomicUsize::new(0),
        };
        let narrowed = narrow(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            vec![
                (
                    host("sandbank"),
                    answering("sandbank", &["Example-org/Example-repo"]),
                ),
                (
                    host("shelter"),
                    answering("shelter", &["example-org/example-repo"]),
                ),
            ],
            ACCESS_BUDGET,
        )
        .await;

        assert_eq!(directory.calls.load(Ordering::SeqCst), 1);
        assert!(!narrowed.partial);
        let (_, sandbank) = narrowed.reports.first().unwrap();
        let (_, shelter) = narrowed.reports.last().unwrap();
        assert_eq!(agent_repos(sandbank), vec!["Example-org/Example-repo"]);
        assert_eq!(agent_repos(shelter), vec!["example-org/example-repo"]);
    }

    #[tokio::test]
    async fn past_the_cap_every_host_whose_rows_got_no_check_makes_the_answer_partial() {
        // One repo per host, so the round-robin gives the first thirty-two
        // hosts one check each and every later host none.
        let directory = Everything {
            calls: AtomicUsize::new(0),
        };
        let hosts = MAX_REPOS_PER_QUERY + 8;
        let reports: Vec<(HostId, HostState)> = (0..hosts)
            .map(|i| {
                let name = format!("host-{i:02}");
                let slug = format!("example-org/h{i:02}");
                (host(&name), answering(&name, &[slug.as_str()]))
            })
            .collect();

        let narrowed = narrow(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            reports,
            ACCESS_BUDGET,
        )
        .await;

        assert_eq!(directory.calls.load(Ordering::SeqCst), MAX_REPOS_PER_QUERY);
        for (i, (_, state)) in narrowed.reports.iter().enumerate() {
            let expected = usize::from(i < MAX_REPOS_PER_QUERY);
            assert_eq!(agent_repos(state).len(), expected, "host {i}");
        }
        assert!(
            narrowed.partial,
            "hosts past the cap read as idle without the notice"
        );
    }

    #[test]
    fn fair_order_takes_turns_and_never_repeats_a_repo() {
        let a = [repo("o/a1"), repo("o/shared"), repo("o/a2")];
        let b = [repo("o/b1"), repo("o/shared"), repo("o/b2")];
        let c: [RepoSlug; 0] = [];

        let order = fair_order([a.iter(), b.iter(), c.iter()]);
        let order: Vec<&str> = order.iter().map(RepoSlug::as_str).collect();

        // Round one: each source's first repo. Round two: a queues `o/shared`,
        // and b — whose next repo is that same `o/shared` — skips it and still
        // gets its turn with `o/b2`. Round three: a's last repo.
        assert_eq!(order, vec!["o/a1", "o/b1", "o/shared", "o/b2", "o/a2"]);
    }

    /// Every check takes `delay` and grants; records the most checks that
    /// were ever in flight at once.
    struct Slow {
        delay: Duration,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
    }

    impl Slow {
        fn new(delay: Duration) -> Self {
            Self {
                delay,
                in_flight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            }
        }
    }

    impl Directory for Slow {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Ok("reader".to_owned())
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(true)
        }
        async fn can_read(&self, _token: &SecretToken, _repo: &RepoSlug) -> RepoAccess {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            RepoAccess::Readable
        }
    }

    #[tokio::test]
    async fn slow_checks_run_concurrently_within_the_bound_and_all_still_count() {
        // One after another, sixteen checks at 250ms each took 4s. Eight at a
        // time is two rounds.
        let delay = Duration::from_millis(250);
        let directory = Slow::new(delay);
        let names: Vec<String> = (0..16).map(|i| format!("example-org/s{i:02}")).collect();
        let repos: Vec<&str> = names.iter().map(String::as_str).collect();

        let started = Instant::now();
        let narrowed = narrow(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            vec![(host("sandbank"), answering("sandbank", &repos))],
            ACCESS_BUDGET,
        )
        .await;
        let elapsed = started.elapsed();

        assert!(
            elapsed < delay * 8,
            "checks ran one after another: {elapsed:?}"
        );
        assert!(!narrowed.partial);
        let (_, state) = narrowed.reports.first().unwrap();
        assert_eq!(agent_repos(state).len(), 16);
        let peak = directory.peak.load(Ordering::SeqCst);
        assert!(peak <= MAX_CONCURRENT_CHECKS, "{peak} checks at once");
        assert!(peak > 1, "never ran two checks at once");
    }

    #[tokio::test]
    async fn a_slow_github_does_not_hold_the_projects_reply_past_whos_wait() {
        // `who` waits three seconds for this reply before it renders
        // ungrouped. Six mapped repos at 900ms each took 5.4s one after
        // another, so even a reader who can read all of them lost grouping.
        let directory = Slow::new(Duration::from_millis(900));
        let projects = vec![
            project(
                "example-repo",
                &["example-org/e1", "example-org/e2", "example-org/e3"],
            ),
            project(
                "example-tools",
                &["example-org/k1", "example-org/k2", "example-org/k3"],
            ),
        ];

        let started = Instant::now();
        let filtered = filter_projects(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            projects.clone(),
            ACCESS_BUDGET,
        )
        .await;
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_millis(2500),
            "the Projects reply took {elapsed:?}"
        );
        assert_eq!(filtered, projects);
    }

    /// Grants the `instant` repos at once and never answers for any other;
    /// counts every call it receives.
    struct Mixed {
        instant: Vec<String>,
        calls: AtomicUsize,
    }

    impl Directory for Mixed {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Ok("reader".to_owned())
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(true)
        }
        async fn can_read(&self, _token: &SecretToken, repo: &RepoSlug) -> RepoAccess {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.instant.iter().any(|slug| slug == repo.as_str()) {
                RepoAccess::Readable
            } else {
                std::future::pending::<RepoAccess>().await
            }
        }
    }

    fn slugs(prefix: &str, count: usize) -> Vec<String> {
        (0..count)
            .map(|i| format!("example-org/{prefix}{i:02}"))
            .collect()
    }

    #[tokio::test]
    async fn checks_never_started_before_the_deadline_are_withheld_not_granted() {
        // Four instant grants take the first slots and finish, eight hung
        // checks fill the bound and time out at the deadline, and the last
        // eight are still queued when it passes — so they must never start,
        // and must never be granted.
        let instant = slugs("fast", 4);
        let hung = slugs("hung", 16);
        let directory = Mixed {
            instant: instant.clone(),
            calls: AtomicUsize::new(0),
        };
        let repos: Vec<&str> = instant.iter().chain(&hung).map(String::as_str).collect();

        let narrowed = tokio::time::timeout(
            Duration::from_secs(5),
            narrow(
                &directory,
                &token("ghp_reader"),
                &host("reader"),
                vec![(host("sandbank"), answering("sandbank", &repos))],
                Duration::from_millis(150),
            ),
        )
        .await
        .expect("the deadline did not end the request");

        let (_, state) = narrowed.reports.first().unwrap();
        assert_eq!(agent_repos(state), instant, "only the instant grants count");
        assert!(narrowed.partial);
        assert_eq!(
            directory.calls.load(Ordering::SeqCst),
            12,
            "4 instant + 8 hung started; the 8 queued past the deadline never start"
        );
    }

    #[tokio::test]
    async fn a_passed_deadline_starts_no_check_even_one_that_would_answer_at_once() {
        // `timeout_at` polls the check before it looks at the clock, so an
        // answer that is ready at once — a cache hit — would still come back
        // after the deadline. Only the guard before starting a check stops it.
        let everything = Everything {
            calls: AtomicUsize::new(0),
        };
        let zero_budget = narrow(
            &everything,
            &token("ghp_reader"),
            &host("reader"),
            vec![(
                host("sandbank"),
                answering(
                    "sandbank",
                    &["example-org/a", "example-org/b", "example-org/c"],
                ),
            )],
            Duration::ZERO,
        )
        .await;
        assert_eq!(everything.calls.load(Ordering::SeqCst), 0);
        assert!(zero_budget.partial);
        let (_, state) = zero_budget.reports.first().unwrap();
        assert!(agent_repos(state).is_empty(), "granted with no budget");

        // Instant answers queued behind eight hung checks: by the time a slot
        // frees, the deadline has passed.
        let hung = slugs("hung", 8);
        let queued = slugs("cached", 4);
        let directory = Mixed {
            instant: queued.clone(),
            calls: AtomicUsize::new(0),
        };
        let repos: Vec<&str> = hung.iter().chain(&queued).map(String::as_str).collect();

        let behind = tokio::time::timeout(
            Duration::from_secs(5),
            narrow(
                &directory,
                &token("ghp_reader"),
                &host("reader"),
                vec![(host("sandbank"), answering("sandbank", &repos))],
                Duration::from_millis(100),
            ),
        )
        .await
        .expect("the deadline did not end the request");

        let (_, state) = behind.reports.first().unwrap();
        assert!(
            agent_repos(state).is_empty(),
            "an instant answer was granted after the deadline: {:?}",
            agent_repos(state)
        );
        assert_eq!(directory.calls.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn a_resolver_that_never_answers_is_abandoned_at_the_budget() {
        let budget = Duration::from_millis(50);
        let started = Instant::now();
        // The outer timeout is far above the budget and exists only so that a
        // regression that loses the budget fails this test instead of hanging
        // CI: `Hangs` never answers on its own.
        let narrowed = tokio::time::timeout(
            Duration::from_secs(5),
            narrow(
                &Hangs,
                &token("ghp_reader"),
                &host("reader"),
                vec![(
                    host("sandbank"),
                    answering(
                        "sandbank",
                        &["example-org/sealmesh", "example-org/example-repo"],
                    ),
                )],
                budget,
            ),
        )
        .await
        .expect("the budget did not bound a resolver that never answers");

        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the budget did not bound the wait: {:?}",
            started.elapsed()
        );
        assert!(narrowed.partial);
        let (_, state) = narrowed.reports.first().unwrap();
        assert!(agent_repos(state).is_empty(), "failed open on a timeout");
    }

    #[tokio::test]
    async fn every_daemon_is_still_asked_and_told_who_asked_even_when_nothing_is_readable() {
        // Rule 16: the daemon's ledger entry comes from the `Query` frame, so
        // narrowing the answer must not narrow who is asked.
        let registry = Registry::new();
        let (mut rx, _guard) = registry.attach(host("sandbank"));
        let directory = Access::new(&[]);
        let budgets = CheckBudgets::default();
        let reader = host("reader");
        let reader_token = token("ghp_reader");

        let answering_daemon = async {
            let Some(ServerFrame::Query { id, asked_by }) = rx.recv().await else {
                panic!("the daemon was never asked");
            };
            registry.resolve(id, answering("sandbank", &["example-org/private"]));
            asked_by
        };
        let (narrowed, asked_by) = tokio::join!(
            ask_readable(
                &registry,
                &directory,
                &budgets,
                &reader_token,
                &reader,
                FANOUT_TIMEOUT
            ),
            answering_daemon
        );

        assert_eq!(asked_by, reader);
        assert!(narrowed.partial);
        assert_eq!(narrowed.reports.len(), 1, "the machine is still listed");
    }

    fn project(id: &str, repos: &[&str]) -> ProjectEntry {
        ProjectEntry {
            id: sealmesh_wire::ProjectId::try_from(id).unwrap(),
            repos: repos.iter().map(|r| repo(r)).collect(),
        }
    }

    #[tokio::test]
    async fn a_projects_reply_lists_only_the_repos_the_reader_can_read() {
        let directory = Access::new(&["example-org/example-repo"]);
        let filtered = filter_projects(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            vec![project(
                "example-repo",
                &[
                    "example-org/example-repo",
                    "example-org/example-repo-portal",
                ],
            )],
            ACCESS_BUDGET,
        )
        .await;

        assert_eq!(
            filtered,
            vec![project("example-repo", &["example-org/example-repo"])]
        );
    }

    #[tokio::test]
    async fn a_project_whose_repos_are_all_withheld_disappears_rather_than_going_out_empty() {
        let directory = Access {
            unverifiable: &["example-org/example-tools"],
            ..Access::new(&["example-org/example-repo"])
        };
        let filtered = filter_projects(
            &directory,
            &token("ghp_reader"),
            &host("reader"),
            vec![
                project("example-repo", &["example-org/example-repo"]),
                project("example-tools", &["example-org/example-tools"]),
                project("secret", &["example-org/secret"]),
            ],
            ACCESS_BUDGET,
        )
        .await;

        assert_eq!(
            filtered,
            vec![project("example-repo", &["example-org/example-repo"])]
        );
    }

    #[test]
    fn the_unverifiable_log_line_names_repos_only_for_the_routers_own_map() {
        // Distinctive names a real GitHub org would never use, so a mutation
        // that lets a peer-supplied repo slug back into the line cannot hide
        // behind a name that also appears somewhere else in the line.
        let peer_repos = [
            repo("magenta-corp/definitely-a-snapshot-repo"),
            repo("trudy-corp/also-a-snapshot-repo"),
        ];
        let reader = host("reader");
        let budgets = CheckBudgets::new(ChecksPerHour::MIN);

        let line = unverifiable_log_line(Payer::Reader(&budgets), &reader, &peer_repos);

        assert!(
            line.contains(&peer_repos.len().to_string()),
            "expected the count in the line: {line}"
        );
        assert!(
            line.contains("reader"),
            "expected the reader's credential host in the line: {line}"
        );
        for peer_repo in &peer_repos {
            assert!(
                !line.contains(peer_repo.as_str()),
                "a peer repo slug leaked into the log: {line}"
            );
        }

        // The router's own map is a different payer, and is allowed to name
        // its own repos.
        let router_repos = [repo("example-org/router-mapped")];
        let router_line = unverifiable_log_line(Payer::RouterMap, &reader, &router_repos);
        assert!(
            router_line.contains("example-org/router-mapped"),
            "the router's own map should be nameable: {router_line}"
        );
    }

    /// Grants everything, and yields inside `can_read` so a check-and-charge
    /// race that only ever completes back-to-back on one poll has room to
    /// interleave with another in-flight check.
    struct Yields {
        calls: AtomicUsize,
    }

    impl Directory for Yields {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Ok("reader".to_owned())
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(true)
        }
        async fn can_read(&self, _token: &SecretToken, _repo: &RepoSlug) -> RepoAccess {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_micros(50)).await;
            RepoAccess::Readable
        }
    }

    #[tokio::test]
    async fn concurrent_queries_whose_batches_do_not_divide_the_concurrency_never_overspend_the_burst()
     {
        // 7 repos per query does not divide MAX_CONCURRENT_CHECKS (8), and the
        // hostile-daemon and per-query tests above all use batch sizes that
        // do, so a check-then-spend race would land on exactly CHECK_BURST by
        // coincidence there. Running many such queries concurrently against
        // one shared allowance is what actually exercises the race.
        let directory = Arc::new(Yields {
            calls: AtomicUsize::new(0),
        });
        let budgets = Arc::new(CheckBudgets::new(ChecksPerHour::MIN));
        let reader = host("reader");
        let per_query = 7;
        let queries = 20;

        let mut handles = Vec::with_capacity(queries);
        for round in 0..queries {
            let directory = Arc::clone(&directory);
            let budgets = Arc::clone(&budgets);
            let reader = reader.clone();
            handles.push(tokio::spawn(async move {
                super::narrow(
                    &*directory,
                    &budgets,
                    &token("ghp_reader"),
                    &reader,
                    vec![(
                        host("magenta"),
                        answering_all("magenta", &fresh(round, "magenta", per_query)),
                    )],
                    ACCESS_BUDGET,
                )
                .await
            }));
        }
        for handle in handles {
            handle.await.expect("a query task panicked");
        }

        let spent = directory.calls.load(Ordering::SeqCst);
        assert!(
            spent <= burst(),
            "the reader's uncached GitHub calls exceeded the burst: {spent} > {}",
            burst()
        );
    }
}
