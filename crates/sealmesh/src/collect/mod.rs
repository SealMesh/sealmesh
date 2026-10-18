pub mod claude_sessions;
pub mod declared;
pub mod fs;
pub mod generic;
pub mod git;
pub mod job;
pub mod link;
pub mod liveness;
pub mod provider;
pub mod providers;
pub mod roots;
pub mod transcript;

use std::{collections::BTreeSet, path::Path};

use jiff::{SignedDuration, Timestamp};

use crate::{
    config::Gate,
    wire::{self, BranchName, HostId, MAX_AGENTS, RepoSlug, SCHEMA_VERSION, TaskProgress},
};

use liveness::Liveness;
use provider::{Priority, Provider, ProviderId, ScanCtx};

#[derive(Debug, Clone)]
pub struct Candidate {
    pub id: wire::JobId,
    /// Which tool this row is. Every provider knows what it read, so this is
    /// not an `Option` here — the `Option` lives only on the wire, and only for
    /// peers that predate the field.
    pub tool: wire::ToolId,
    pub work_dir: std::path::PathBuf,
    pub session_id: Option<String>,
    pub children: Vec<job::RawChild>,
    pub run_state: wire::RunState,
    pub started_at: Timestamp,
    pub updated_at: Timestamp,
    pub declared_branch: Option<wire::BranchName>,
    pub declared_pr: Option<u32>,
    /// Task counts a provider supplied, from a declared record or from the
    /// vendor's own files. Named a hint rather than `declared_tasks` because it
    /// stopped being declared-only when `task_progress` moved out of the funnel.
    pub tasks_hint: Option<TaskProgress>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoFacts {
    pub slug: RepoSlug,
    pub branch: Option<BranchName>,
    pub ahead: Option<u32>,
    pub dirty_files: Option<u32>,
}

pub trait RepoResolver {
    fn resolve(&self, dir: &Path) -> Option<RepoFacts>;
}

#[derive(Debug, Clone)]
pub struct Options {
    pub integration_branch: String,
    pub ticket_prefixes: Vec<String>,
    pub declared_dir: Option<std::path::PathBuf>,
    pub declared_ttl: SignedDuration,
}

/// Fold every provider's view of the same agent into one row.
///
/// The comparison is `(priority, updated_at, provider)`, in that order, and all
/// three matter. Priority first, so a newer session file still loses to the job
/// file that knows more. `updated_at` second, to break ties within one
/// provider. The provider id last, because without it an exact tie was settled
/// by whichever candidate happened to be pushed first — which depended on the
/// order roots sorted in, and would have flipped silently under this refactor.
fn merge(candidates: Vec<(ProviderId, Priority, Candidate)>) -> Vec<(ProviderId, Candidate)> {
    let mut best: std::collections::BTreeMap<
        (wire::ToolId, wire::JobId),
        (ProviderId, Priority, Candidate),
    > = std::collections::BTreeMap::new();
    for (source, priority, mut candidate) in candidates {
        // Keyed on the tool as well as the id. Two providers seeing the same
        // agent is the case this fold exists for, and they agree on the tool.
        // Two *different* tools landing on the same id is the opposite: both
        // derive an id from a uuid or a hash, so a collision is rare and not
        // impossible, and fusing two unrelated agents into one row is silent.
        let key = (candidate.tool.clone(), candidate.id.clone());
        if let Some((_, _, held)) = best.get(&key) {
            candidate.started_at = candidate.started_at.min(held.started_at);
        }
        match best.get_mut(&key) {
            Some((held_source, held_priority, held_candidate))
                if (*held_priority, held_candidate.updated_at, &*held_source)
                    >= (priority, candidate.updated_at, &source) =>
            {
                held_candidate.started_at = candidate.started_at;
            }
            _ => {
                best.insert(key, (source, priority, candidate));
            }
        }
    }
    best.into_values()
        .map(|(source, _, candidate)| (source, candidate))
        .collect()
}

#[derive(Debug)]
pub struct Decision {
    pub id: wire::JobId,
    pub source: ProviderId,
    pub work_dir: std::path::PathBuf,
    pub outcome: Outcome,
}

#[derive(Debug)]
pub enum Outcome {
    Published(Box<wire::Agent>),
    PathDenied,
    NoRepo,
    RepoDenied(RepoSlug),
}

fn candidates(
    providers: &[Box<dyn Provider>],
    procs: &dyn Liveness,
    now: Timestamp,
) -> Vec<(ProviderId, Candidate)> {
    let live_sessions = live_session_ids(providers, procs);
    let ctx = ScanCtx {
        now,
        procs,
        live_sessions: &live_sessions,
    };

    let mut found = Vec::new();
    for source in providers {
        let id = source.id();
        let priority = source.priority();
        found.extend(
            source
                .scan(&ctx)
                .into_iter()
                .map(|candidate| (id.clone(), priority, candidate)),
        );
    }

    merge(found)
}

/// Every provider's live sessions, gathered before any scan runs.
///
/// The order matters and is the reason this is its own pass. A declared record
/// naming a parent is checked against this set, and if it were built while
/// scanning, whether a record survived would depend on where its parent's
/// provider sat in the list. Gathering first also means a non-Claude tool's
/// session can keep a declared record alive, which is the case `parent` exists
/// for and could not serve while the Claude session reader was the only source.
pub fn live_session_ids(providers: &[Box<dyn Provider>], procs: &dyn Liveness) -> BTreeSet<String> {
    providers
        .iter()
        .flat_map(|source| source.live_sessions(procs))
        .map(|session| session.id)
        .collect()
}

pub fn examine(
    providers: &[Box<dyn Provider>],
    gate: &Gate,
    resolver: &dyn RepoResolver,
    options: &Options,
) -> Vec<Decision> {
    examine_with(
        providers,
        gate,
        resolver,
        options,
        &liveness::ProcessTable::read(),
        Timestamp::now(),
    )
}

pub fn examine_with(
    providers: &[Box<dyn Provider>],
    gate: &Gate,
    resolver: &dyn RepoResolver,
    options: &Options,
    procs: &dyn Liveness,
    now: Timestamp,
) -> Vec<Decision> {
    let mut decisions = Vec::new();

    let found = candidates(providers, procs, now);
    for (source, candidate) in found {
        let work_dir = candidate.work_dir.clone();
        let outcome = match gate.allowed_dir(&candidate.work_dir) {
            None => Outcome::PathDenied,
            Some(dir) => match resolver.resolve(&dir) {
                None => Outcome::NoRepo,
                Some(facts) if !gate.allows_repo(&facts.slug) => Outcome::RepoDenied(facts.slug),
                Some(facts) => {
                    let branch = candidate.declared_branch.or(facts.branch);
                    let tasks = candidate.tasks_hint;
                    let ticket = branch
                        .as_ref()
                        .and_then(|b| link::ticket_from_branch(b, &options.ticket_prefixes));
                    let pr = link::pr_from_children(&candidate.children, &facts.slug)
                        .or(candidate.declared_pr);

                    Outcome::Published(Box::new(wire::Agent {
                        id: candidate.id.clone(),
                        repo: facts.slug,
                        run_state: candidate.run_state,
                        branch,
                        ticket,
                        ahead: facts.ahead,
                        dirty_files: facts.dirty_files,
                        tasks,
                        pr,
                        tool: Some(candidate.tool.clone()),
                        started_at: candidate.started_at,
                        updated_at: candidate.updated_at,
                    }))
                }
            },
        };
        decisions.push(Decision {
            id: candidate.id,
            source,
            work_dir,
            outcome,
        });
    }
    decisions
}

pub fn collect(
    providers: &[Box<dyn Provider>],
    gate: &Gate,
    resolver: &dyn RepoResolver,
    options: &Options,
) -> Vec<wire::Agent> {
    let mut agents: Vec<wire::Agent> = examine(providers, gate, resolver, options)
        .into_iter()
        .filter_map(|decision| match decision.outcome {
            Outcome::Published(agent) => Some(*agent),
            _ => None,
        })
        .collect();

    agents.sort_by(|a, b| b.updated_at.cmp(&a.updated_at).then(a.id.cmp(&b.id)));
    agents.truncate(MAX_AGENTS);
    agents
}

pub fn snapshot(host: HostId, collected_at: Timestamp, agents: Vec<wire::Agent>) -> wire::Snapshot {
    wire::Snapshot {
        schema: SCHEMA_VERSION,
        host,
        collected_at,
        agents,
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

    use std::{
        collections::{BTreeMap, BTreeSet},
        path::PathBuf,
    };

    use super::*;

    const CANARY: &str = "PASTED-SLACK-THREAD-NAMING-A-THIRD-PARTY";

    const FIXTURE_PROC_START: i64 = 1_787_510_713;

    struct Procs(BTreeMap<u32, i64>);

    impl liveness::Liveness for Procs {
        fn started_at_secs(&self, pid: u32) -> Option<i64> {
            self.0.get(&pid).copied()
        }
    }

    fn procs(pids: &[u32]) -> Procs {
        Procs(pids.iter().map(|p| (*p, FIXTURE_PROC_START)).collect())
    }

    struct Machine {
        base: PathBuf,
        slugs: BTreeMap<PathBuf, RepoFacts>,
    }

    impl Machine {
        fn new(name: &str) -> Self {
            let base = std::env::temp_dir().join(format!("sealmesh-collect-{name}"));
            std::fs::remove_dir_all(&base).ok();
            std::fs::create_dir_all(&base).unwrap();
            Self {
                base,
                slugs: BTreeMap::new(),
            }
        }

        fn checkout(&mut self, rel: &str, slug: &str, branch: &str) -> PathBuf {
            let dir = self.base.join(rel);
            std::fs::create_dir_all(&dir).unwrap();
            let canonical = dir.canonicalize().unwrap();
            self.slugs.insert(
                canonical,
                RepoFacts {
                    slug: RepoSlug::try_from(slug).unwrap(),
                    branch: Some(BranchName::try_from(branch).unwrap()),
                    ahead: Some(3),
                    dirty_files: Some(7),
                },
            );
            dir
        }

        fn job(&self, root: &str, id: &str, cwd: &Path, state: &str, tempo: &str) {
            let dir = self.base.join(root).join("jobs").join(id);
            std::fs::create_dir_all(&dir).unwrap();
            let json = serde_json::json!({
                "state": state,
                "tempo": tempo,
                "cwd": cwd,
                "sessionId": format!("session-{id}"),
                "createdAt": "2026-08-10T17:41:03.591Z",
                "updatedAt": "2026-08-10T19:53:28.829Z",
                "intent": CANARY,
                "detail": CANARY,
                "name": CANARY,
                "nameSource": "auto",
                "children": [{"kind": "pr", "href": "https://github.com/example-org/example-repo/pull/627"}],
            });
            std::fs::write(dir.join("state.json"), json.to_string()).unwrap();
        }

        fn session(&self, root: &str, pid: u32, session_id: &str, cwd: &Path, status: &str) {
            let dir = self.base.join(root).join("sessions");
            std::fs::create_dir_all(&dir).unwrap();
            let json = serde_json::json!({
                "pid": pid,
                "sessionId": session_id,
                "cwd": cwd,
                "startedAt": 1_787_510_714_656_i64,
                "updatedAt": 1_787_510_719_656_i64,
                "procStart": "Sun Aug 23 18:45:13 2026",
                "kind": "interactive",
                "status": status,
                "name": CANARY,
                "nameSource": "user",
                "waitingFor": CANARY,
                "messagingSocketPath": "/tmp/cc-socks/1.sock",
            });
            std::fs::write(dir.join(format!("{pid}.json")), json.to_string()).unwrap();
        }

        fn declare(&self, id: &str, worktree: &Path, state: &str, extra: serde_json::Value) {
            let dir = self.base.join("declared");
            std::fs::create_dir_all(&dir).unwrap();
            let mut json = serde_json::json!({
                "id": id,
                "worktree": worktree,
                "run_state": state,
                "started_at": "2026-08-24T11:41:03.591Z",
                "updated_at": "2026-08-24T11:55:19.004Z",
            });
            if let Some(fields) = extra.as_object() {
                for (k, v) in fields {
                    json[k] = v.clone();
                }
            }
            std::fs::write(dir.join(format!("{id}.json")), json.to_string()).unwrap();
        }

        fn half_started_job(&self, root: &str, id: &str) {
            std::fs::create_dir_all(self.base.join(root).join("jobs").join(id).join("tmp"))
                .unwrap();
        }

        fn tasks(&self, root: &str, session_id: &str, statuses: &[&str]) {
            let dir = self.base.join(root).join("tasks").join(session_id);
            std::fs::create_dir_all(&dir).unwrap();
            for (i, status) in statuses.iter().enumerate() {
                let json = serde_json::json!({
                    "id": i.to_string(),
                    "subject": CANARY,
                    "activeForm": CANARY,
                    "status": status,
                });
                std::fs::write(dir.join(format!("{i}.json")), json.to_string()).unwrap();
            }
        }

        fn roots(&self) -> Vec<roots::Root> {
            roots::discover(Some(&self.base), &[], None)
        }

        /// The same list production builds, from the same factory — so a test
        /// cannot pass because the harness assembled providers differently
        /// from the daemon.
        fn providers(&self, options: &Options) -> Vec<Box<dyn Provider>> {
            providers::built_in(
                self.roots(),
                options.declared_dir.clone(),
                options.declared_ttl,
            )
        }

        fn gate(&self, allowed_dirs: &[&str], allowed_repos: &[&str]) -> Gate {
            Gate::new(
                allowed_dirs
                    .iter()
                    .map(|d| self.base.join(d).canonicalize().unwrap())
                    .collect(),
                allowed_repos
                    .iter()
                    .map(|s| RepoSlug::try_from(*s).unwrap())
                    .collect::<BTreeSet<_>>(),
            )
        }
    }

    impl Drop for Machine {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.base).ok();
        }
    }

    impl RepoResolver for Machine {
        fn resolve(&self, dir: &Path) -> Option<RepoFacts> {
            self.slugs.get(dir).cloned()
        }
    }

    fn machine(name: &str) -> Machine {
        let mut m = Machine::new(name);
        let example_repo = m.checkout(
            "work/example-repo",
            "example-org/example-repo",
            "feature/eng-557-qa",
        );
        let acme = m.checkout("clients/acme-litigation", "acme/matter", "main");
        let fork = m.checkout(
            "work/example-repo-fork",
            "someone-else/example-repo",
            "main",
        );

        m.job(".claude", "1e631e34", &example_repo, "working", "blocked");
        m.job(
            ".claude-example-org",
            "949c7c62",
            &example_repo,
            "working",
            "active",
        );
        m.job(".claude", "aaaa1111", &acme, "working", "active");
        m.job(".claude", "bbbb2222", &fork, "working", "active");
        m.half_started_job(".claude-example-org", "cccc3333");
        m.session(
            ".claude",
            64653,
            "d4d4d4d4-d94b-4d7e-9f02-5b09827a67f9",
            &example_repo,
            "waiting",
        );
        m.session(
            ".claude",
            9999,
            "e5e5e5e5-d94b-4d7e-9f02-5b09827a67f9",
            &example_repo,
            "busy",
        );
        m.tasks(
            ".claude",
            "session-1e631e34",
            &["completed", "completed", "pending"],
        );
        m
    }

    fn options() -> Options {
        Options {
            integration_branch: "dev".to_owned(),
            ticket_prefixes: vec!["ENG".to_owned()],
            declared_dir: None,
            declared_ttl: jiff::SignedDuration::from_secs(900),
        }
    }

    fn options_declaring(m: &Machine) -> Options {
        Options {
            declared_dir: Some(m.base.join("declared")),
            ..options()
        }
    }

    fn fixture_now() -> Timestamp {
        "2026-08-24T12:00:00Z".parse().unwrap()
    }

    fn collected(m: &Machine) -> Vec<wire::Agent> {
        examined(m)
            .into_iter()
            .filter_map(|d| match d.outcome {
                Outcome::Published(agent) => Some(*agent),
                _ => None,
            })
            .collect()
    }

    fn examined(m: &Machine) -> Vec<Decision> {
        examined_with(m, &options())
    }

    fn examined_with(m: &Machine, options: &Options) -> Vec<Decision> {
        examine_with(
            &m.providers(options),
            &m.gate(&["work"], &["example-org/example-repo"]),
            m,
            options,
            &procs(&[64653]),
            fixture_now(),
        )
    }

    #[test]
    fn no_canary_survives_collection() {
        let m = machine("canary");
        let snapshot = snapshot(
            HostId::try_from("sandbank").unwrap(),
            "2026-08-10T19:56:51.322Z".parse().unwrap(),
            collected(&m),
        );
        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(!json.is_empty());
        assert!(!json.contains(CANARY), "canary reached the wire: {json}");
    }

    #[test]
    fn no_canary_survives_the_diff_into_events() {
        use crate::events::transitions;

        let m = machine("canary-events");
        let host = HostId::try_from("sandbank").unwrap();
        let at: Timestamp = "2026-08-10T19:56:51.322Z".parse().unwrap();
        let agents = collected(&m);
        assert!(!agents.is_empty(), "the fixture collected nothing");

        let earlier: Vec<wire::Agent> = agents
            .iter()
            .cloned()
            .map(|mut a| {
                a.run_state = wire::RunState::Idle;
                a.pr = None;
                a
            })
            .collect();

        let mut events = transitions(&[], &agents, &host, at);
        events.extend(transitions(&earlier, &agents, &host, at));
        events.extend(transitions(&agents, &[], &host, at));
        assert!(events.len() >= 4, "{events:#?}");

        let json = serde_json::to_string(&events).unwrap();
        assert!(!json.contains(CANARY), "canary reached an event: {json}");
        assert!(!json.contains("/Users/"), "{json}");
        assert!(!json.contains(&m.base.display().to_string()), "{json}");
        assert!(!json.contains("acme"), "{json}");
    }

    #[test]
    fn no_absolute_path_reaches_the_wire() {
        let m = machine("paths");
        let snapshot = snapshot(
            HostId::try_from("sandbank").unwrap(),
            "2026-08-10T19:56:51.322Z".parse().unwrap(),
            collected(&m),
        );
        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(!json.contains("/Users/"), "{json}");
        assert!(!json.contains(&m.base.display().to_string()), "{json}");
        assert!(!json.contains("acme"), "{json}");
    }

    #[test]
    fn only_the_shared_repo_in_an_allowed_path_is_published() {
        let m = machine("gates");
        let agents = collected(&m);
        assert_eq!(agents.len(), 3, "{agents:#?}");
        assert!(
            agents
                .iter()
                .all(|a| a.repo.as_str() == "example-org/example-repo")
        );
        let ids: BTreeSet<_> = agents.iter().map(|a| a.id.as_str().to_owned()).collect();
        assert!(ids.contains("1e631e34"));
        assert!(ids.contains("949c7c62"));
        assert!(ids.contains("d4d4d4d4"), "the live session is missing");
        assert!(!ids.contains("aaaa1111"));
        assert!(!ids.contains("bbbb2222"));
        assert!(!ids.contains("e5e5e5e5"), "a dead session was published");
    }

    #[test]
    fn a_machine_with_only_sessions_still_reports_its_agents() {
        let mut m = Machine::new("sessionsonly");
        let example_repo = m.checkout(
            "work/example-repo",
            "example-org/example-repo",
            "feature/eng-557-qa",
        );
        m.session(
            ".claude",
            64653,
            "d4d4d4d4-d94b-4d7e-9f02-5b09827a67f9",
            &example_repo,
            "busy",
        );

        let agents = collected(&m);
        assert_eq!(agents.len(), 1, "{agents:#?}");
        assert_eq!(agents.first().unwrap().run_state, wire::RunState::Working);
    }

    #[test]
    fn a_session_waiting_on_a_person_reaches_the_wire_as_needing_input() {
        let m = machine("waiting");
        let agents = collected(&m);
        let session = agents.iter().find(|a| a.id.as_str() == "d4d4d4d4").unwrap();
        assert_eq!(session.run_state, wire::RunState::NeedsInput);
    }

    #[test]
    fn every_decision_names_the_adapter_that_saw_it() {
        let m = machine("provenance");
        let by_id: BTreeMap<String, ProviderId> = examined(&m)
            .into_iter()
            .map(|d| (d.id.as_str().to_owned(), d.source))
            .collect();
        assert_eq!(by_id.get("1e631e34"), Some(&ProviderId::CLAUDE_JOB));
        assert_eq!(by_id.get("d4d4d4d4"), Some(&ProviderId::CLAUDE_SESSION));
    }

    #[test]
    fn a_declared_record_goes_through_both_gates() {
        let mut m = Machine::new("declared-gates");
        let example_repo = m.checkout(
            "work/example-repo",
            "example-org/example-repo",
            "feature/eng-557-qa",
        );
        let acme = m.checkout("clients/acme-litigation", "acme/matter", "main");
        let fork = m.checkout(
            "work/example-repo-fork",
            "someone-else/example-repo",
            "main",
        );

        m.declare("aaaa1111", &example_repo, "working", serde_json::json!({}));
        m.declare("bbbb2222", &acme, "working", serde_json::json!({}));
        m.declare("cccc3333", &fork, "working", serde_json::json!({}));

        let agents: BTreeSet<String> = examined_with(&m, &options_declaring(&m))
            .into_iter()
            .filter_map(|d| match d.outcome {
                Outcome::Published(a) => Some(a.id.as_str().to_owned()),
                _ => None,
            })
            .collect();
        assert_eq!(agents.len(), 1, "{agents:?}");
        assert!(agents.contains("aaaa1111"));
    }

    #[test]
    fn doctors_liveness_list_agrees_with_the_collectors() {
        let mut m = Machine::new("doctor-liveness");
        let example_repo = m.checkout(
            "work/example-repo",
            "example-org/example-repo",
            "feature/eng-557-qa",
        );
        let parent = "d4d4d4d4-d94b-4d7e-9f02-5b09827a67f9";
        m.session(".claude", 64653, parent, &example_repo, "busy");
        m.declare(
            "aaaa1111",
            &example_repo,
            "working",
            serde_json::json!({ "parent": parent }),
        );

        let options = options_declaring(&m);
        let live = live_session_ids(&m.providers(&options), &procs(&[64653]));
        assert!(
            live.contains(parent),
            // The count, not the ids. A session id is what `--parent` uses to
            // prove liveness, so it does not belong in a panic message that
            // ends up in CI output — and the count is the more useful signal
            // anyway: it separates "nothing was alive" from "something was,
            // and it was not this".
            "doctor would not see the parent as live ({} sessions found)",
            live.len()
        );

        let reads = declared::read_all(
            &options.declared_dir.clone().unwrap(),
            options.declared_ttl,
            fixture_now(),
            &declared::LiveSessions(&live),
        );
        assert!(
            reads.iter().all(|r| r.outcome.is_ok()),
            "doctor refused a record: {reads:#?}"
        );

        let published: BTreeSet<String> = examined_with(&m, &options)
            .into_iter()
            .filter_map(|d| match d.outcome {
                Outcome::Published(a) => Some(a.id.as_str().to_owned()),
                _ => None,
            })
            .collect();
        assert!(published.contains("aaaa1111"), "{published:?}");
    }

    fn only_published(m: &Machine, options: &Options) -> wire::Agent {
        let agents = examined_with(m, options);
        let published: Vec<_> = agents
            .iter()
            .filter_map(|d| match &d.outcome {
                Outcome::Published(a) => Some(a),
                _ => None,
            })
            .collect();
        assert_eq!(published.len(), 1, "{agents:?}");
        (***published.first().unwrap()).clone()
    }

    #[test]
    fn a_declared_record_takes_its_repo_facts_from_git_not_from_the_writer() {
        let mut m = Machine::new("declared-facts");
        let example_repo = m.checkout(
            "work/example-repo",
            "example-org/example-repo",
            "feature/eng-557-qa",
        );
        m.declare(
            "aaaa1111",
            &example_repo,
            "needs_input",
            serde_json::json!({ "tasks": {"completed": 2, "total": 5} }),
        );

        let agent = only_published(&m, &options_declaring(&m));

        assert_eq!(agent.repo.as_str(), "example-org/example-repo");
        assert_eq!(agent.ahead, Some(3));
        assert_eq!(agent.dirty_files, Some(7));
        assert_eq!(agent.tasks, Some(TaskProgress::new(2, 5).unwrap()));
        assert_eq!(agent.run_state, wire::RunState::NeedsInput);
    }

    #[test]
    fn a_declared_branch_beats_the_worktrees_synthetic_head() {
        let mut m = Machine::new("declared-branch-wins");
        let example_repo = m.checkout(
            "work/example-repo",
            "example-org/example-repo",
            "worktree-agent-a7931acbbe3d741a5",
        );
        m.declare(
            "aaaa1111",
            &example_repo,
            "working",
            serde_json::json!({ "branch": "feature/eng-612-dispatch-records" }),
        );

        let agent = only_published(&m, &options_declaring(&m));

        assert_eq!(
            agent.branch.as_ref().map(wire::BranchName::as_str),
            Some("feature/eng-612-dispatch-records")
        );
        assert_eq!(
            agent.ticket.as_ref().map(ToString::to_string).as_deref(),
            Some("ENG-612"),
            "the ticket is parsed from the branch, so it follows the override"
        );
        assert_eq!(agent.ahead, Some(3));
        assert_eq!(agent.dirty_files, Some(7));
    }

    #[test]
    fn a_declared_record_without_a_branch_still_gets_the_worktrees() {
        let mut m = Machine::new("declared-branch-fallback");
        let example_repo = m.checkout(
            "work/example-repo",
            "example-org/example-repo",
            "feature/eng-557-qa",
        );
        m.declare("aaaa1111", &example_repo, "working", serde_json::json!({}));

        let agent = only_published(&m, &options_declaring(&m));

        assert_eq!(
            agent.branch.as_ref().map(wire::BranchName::as_str),
            Some("feature/eng-557-qa")
        );
        assert_eq!(
            agent.ticket.as_ref().map(ToString::to_string).as_deref(),
            Some("ENG-557")
        );
    }

    #[test]
    fn the_earliest_start_wins_even_when_the_later_record_outranks_it() {
        let mut m = Machine::new("earliest-start");
        let example_repo = m.checkout(
            "work/example-repo",
            "example-org/example-repo",
            "feature/eng-557-qa",
        );
        m.session(
            ".claude",
            64653,
            "d4d4d4d4-d94b-4d7e-9f02-5b09827a67f9",
            &example_repo,
            "busy",
        );
        m.declare(
            "d4d4d4d4",
            &example_repo,
            "working",
            serde_json::json!({"tool": "claude-code"}),
        );

        let mut options = options_declaring(&m);
        options.declared_ttl = jiff::SignedDuration::from_hours(24 * 365);
        let agent = only_published(&m, &options);

        assert_eq!(agent.run_state, wire::RunState::Working);
        let session_start = Timestamp::from_millisecond(1_787_510_714_656).unwrap();
        assert_eq!(
            agent.started_at, session_start,
            "the later record's restamp overwrote the true start"
        );
    }

    #[test]
    fn a_declared_records_parent_governs_its_liveness() {
        let mut m = Machine::new("declared-parent");
        let example_repo = m.checkout(
            "work/example-repo",
            "example-org/example-repo",
            "feature/eng-557-qa",
        );
        m.session(
            ".claude",
            64653,
            "d4d4d4d4-d94b-4d7e-9f02-5b09827a67f9",
            &example_repo,
            "busy",
        );
        m.declare(
            "aaaa1111",
            &example_repo,
            "working",
            serde_json::json!({
                "parent": "d4d4d4d4-d94b-4d7e-9f02-5b09827a67f9",
                "updated_at": "2026-08-24T10:00:00Z"
            }),
        );
        m.declare(
            "bbbb2222",
            &example_repo,
            "working",
            serde_json::json!({ "parent": "0000dead-0000-0000-0000-000000000000" }),
        );

        let ids: BTreeSet<String> = examined_with(&m, &options_declaring(&m))
            .into_iter()
            .map(|d| d.id.as_str().to_owned())
            .collect();
        assert!(
            ids.contains("aaaa1111"),
            "a live parent did not beat the ttl"
        );
        assert!(!ids.contains("bbbb2222"), "a dead parent produced a row");
    }

    #[test]
    fn no_canary_survives_a_declared_record() {
        let mut m = Machine::new("declared-canary");
        let example_repo = m.checkout(
            "work/example-repo",
            "example-org/example-repo",
            "feature/eng-557-qa",
        );
        m.declare(
            "aaaa1111",
            &example_repo,
            "working",
            serde_json::json!({ "note": CANARY, "intent": CANARY }),
        );
        m.declare("bbbb2222", &example_repo, "working", serde_json::json!({}));

        let declaring = options_declaring(&m);
        let agents = examine_with(
            &m.providers(&declaring),
            &m.gate(&["work"], &["example-org/example-repo"]),
            &m,
            &declaring,
            &procs(&[64653]),
            fixture_now(),
        );
        let snapshot = snapshot(
            HostId::try_from("sandbank").unwrap(),
            fixture_now(),
            agents
                .into_iter()
                .filter_map(|d| match d.outcome {
                    Outcome::Published(a) => Some(*a),
                    _ => None,
                })
                .collect(),
        );
        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(!json.contains(CANARY), "canary reached the wire: {json}");
        assert!(!json.contains("aaaa1111"), "{json}");
        assert!(json.contains("bbbb2222"), "{json}");
    }

    #[test]
    fn a_session_that_is_also_a_job_is_one_row_not_two() {
        let mut m = Machine::new("merge");
        let example_repo = m.checkout(
            "work/example-repo",
            "example-org/example-repo",
            "feature/eng-557-qa",
        );
        m.job(".claude", "d4d4d4d4", &example_repo, "working", "blocked");
        m.session(
            ".claude",
            64653,
            "d4d4d4d4-d94b-4d7e-9f02-5b09827a67f9",
            &example_repo,
            "idle",
        );

        let agents = collected(&m);
        assert_eq!(agents.len(), 1, "{agents:#?}");
        assert_eq!(
            agents.first().unwrap().run_state,
            wire::RunState::NeedsInput
        );
    }

    #[test]
    fn agents_from_every_root_are_collected_not_just_the_first() {
        let m = machine("roots");
        assert_eq!(collected(&m).len(), 3);
    }

    #[test]
    fn blocked_carries_through_and_task_counts_come_from_the_agents_own_list() {
        let m = machine("tasks");
        let agents = collected(&m);
        let blocked = agents.iter().find(|a| a.id.as_str() == "1e631e34").unwrap();
        assert_eq!(blocked.run_state, wire::RunState::NeedsInput);
        assert_eq!(blocked.tasks, Some(TaskProgress::new(2, 3).unwrap()));

        let other = agents.iter().find(|a| a.id.as_str() == "949c7c62").unwrap();
        assert_eq!(other.tasks, None);
    }

    /// A stand-in for a provider sealmesh does not ship — a Codex or Qwen
    /// reader someone else wrote. It exists to prove two things the built-in
    /// set cannot: that priority is a number rather than a position, and that a
    /// session from a non-Claude tool can keep a declared record alive.
    struct Fake {
        id: ProviderId,
        priority: Priority,
        sessions: Vec<String>,
        candidates: Vec<Candidate>,
    }

    impl Provider for Fake {
        fn id(&self) -> ProviderId {
            self.id.clone()
        }

        fn priority(&self) -> Priority {
            self.priority
        }

        fn live_sessions(&self, _procs: &dyn Liveness) -> Vec<provider::SessionRef> {
            self.sessions
                .iter()
                .map(|id| provider::SessionRef {
                    tool: wire::ToolId::try_from("codex").unwrap(),
                    id: id.clone(),
                })
                .collect()
        }

        fn scan(&self, _ctx: &ScanCtx<'_>) -> Vec<Candidate> {
            self.candidates.clone()
        }
    }

    fn candidate(id: &str, work_dir: &Path, updated: &str) -> Candidate {
        Candidate {
            id: wire::JobId::try_from(id).unwrap(),
            tool: wire::ToolId::try_from("codex").unwrap(),
            work_dir: work_dir.to_path_buf(),
            session_id: None,
            children: Vec::new(),
            run_state: wire::RunState::Working,
            started_at: "2026-08-24T11:00:00Z".parse().unwrap(),
            updated_at: updated.parse().unwrap(),
            declared_branch: None,
            declared_pr: None,
            tasks_hint: None,
        }
    }

    #[test]
    fn a_declared_record_outranks_a_session_and_loses_to_a_job() {
        // Nothing pinned declared-against-job before, and the relation was an
        // accident of where the variant sat in an enum. It is a number now, so
        // it gets written down.
        let mut m = machine("declared-vs-job");
        let example_repo = m.checkout("work/example-repo", "example-org/example-repo", "main");

        // Same id as the fixture's job `1e631e34`, which reads NeedsInput.
        m.declare("1e631e34", &example_repo, "idle", serde_json::json!({}));

        let options = options_declaring(&m);
        let row = examined_with(&m, &options)
            .into_iter()
            .filter_map(|d| match d.outcome {
                Outcome::Published(a) if a.id.as_str() == "1e631e34" => Some(*a),
                _ => None,
            })
            .next()
            .expect("the shared id vanished");
        assert_eq!(
            row.run_state,
            wire::RunState::NeedsInput,
            "the declared record beat the job file"
        );
    }

    #[test]
    fn an_exact_tie_does_not_depend_on_the_order_providers_are_listed_in() {
        // Same priority, same updated_at. Before the provider id joined the
        // comparison this was settled by whichever candidate happened to be
        // pushed first, which depended on root sort order.
        let dir = std::env::temp_dir();
        let one = candidate("aaaa1111", &dir, "2026-08-24T11:00:00Z");
        let two = candidate("aaaa1111", &dir, "2026-08-24T11:00:00Z");

        let a = (ProviderId::generic("alpha"), Priority::GENERIC, one);
        let b = (ProviderId::generic("beta"), Priority::GENERIC, two);

        let forward = merge(vec![a.clone(), b.clone()]);
        let backward = merge(vec![b, a]);

        assert_eq!(forward.len(), 1);
        assert_eq!(
            forward.first().map(|(id, _)| id),
            backward.first().map(|(id, _)| id),
            "the winner changed when the providers were listed the other way"
        );
    }

    #[test]
    fn a_directory_with_no_repo_is_named_rather_than_published() {
        // `Outcome::NoRepo` had no test at all, and this refactor rewrote the
        // path into it.
        let m = machine("no-repo");
        let loose = m.base.join("work/loose");
        std::fs::create_dir_all(&loose).unwrap();
        let loose = loose.canonicalize().unwrap();
        m.declare("cafe1234", &loose, "working", serde_json::json!({}));

        let options = options_declaring(&m);
        let decisions = examined_with(&m, &options);
        let found = decisions
            .iter()
            .find(|d| d.id.as_str() == "cafe1234")
            .expect("the candidate never reached the gate");
        assert!(
            matches!(found.outcome, Outcome::NoRepo),
            "expected NoRepo, got {:?}",
            found.outcome
        );
    }

    #[test]
    fn a_session_from_another_tool_can_keep_a_declared_record_alive() {
        // The capability the provider split buys: `parent` used to mean "a
        // Claude Code session", because the Claude session reader was the only
        // thing that could vouch for one.
        let mut m = machine("foreign-parent");
        let example_repo = m.checkout("work/example-repo", "example-org/example-repo", "main");
        let parent = "c0dec0de-4b2d-4e91-8a55-1c0de2f6b7aa";

        // Older than the ttl, so only a live parent can save it.
        m.declare(
            "beef0001",
            &example_repo,
            "working",
            serde_json::json!({
                "parent": parent,
                "updated_at": "2026-08-20T09:00:00Z",
            }),
        );

        let options = options_declaring(&m);
        let mut providers = m.providers(&options);

        let without = examine_with(
            &providers,
            &m.gate(&["work"], &["example-org/example-repo"]),
            &m,
            &options,
            &procs(&[64653]),
            fixture_now(),
        );
        assert!(
            !without.iter().any(|d| d.id.as_str() == "beef0001"),
            "a stale record survived with no live parent"
        );

        providers.push(Box::new(Fake {
            id: ProviderId::generic("codex"),
            priority: Priority::GENERIC,
            sessions: vec![parent.to_owned()],
            candidates: Vec::new(),
        }));

        let with = examine_with(
            &providers,
            &m.gate(&["work"], &["example-org/example-repo"]),
            &m,
            &options,
            &procs(&[64653]),
            fixture_now(),
        );
        assert!(
            with.iter().any(|d| d.id.as_str() == "beef0001"),
            "a non-Claude session could not vouch for a parent"
        );
    }

    #[test]
    fn a_provider_from_config_is_gated_exactly_like_a_built_in_one() {
        // The load-bearing property, stated positively for the first time. It
        // was only ever tested as an absence, and an absence cannot tell you
        // whether the gate ran or the candidate never arrived.
        let mut m = machine("generic-gate");
        let example_repo = m.checkout("work/example-repo", "example-org/example-repo", "main");
        let fork = m.checkout(
            "work/example-repo-fork",
            "someone-else/example-repo",
            "main",
        );
        let outside = m.checkout("clients/acme", "example-org/example-repo", "main");

        let providers: Vec<Box<dyn Provider>> = vec![Box::new(Fake {
            id: ProviderId::generic("qwen"),
            priority: Priority::GENERIC,
            sessions: Vec::new(),
            candidates: vec![
                candidate("aaaa0001", &example_repo, "2026-08-24T11:55:00Z"),
                candidate("aaaa0002", &fork, "2026-08-24T11:55:00Z"),
                candidate("aaaa0003", &outside, "2026-08-24T11:55:00Z"),
            ],
        })];

        let decisions = examine_with(
            &providers,
            &m.gate(&["work"], &["example-org/example-repo"]),
            &m,
            &options(),
            &procs(&[]),
            fixture_now(),
        );

        let by_id: BTreeMap<String, &Outcome> = decisions
            .iter()
            .map(|d| (d.id.to_string(), &d.outcome))
            .collect();

        assert!(
            matches!(by_id["aaaa0001"], Outcome::Published(_)),
            "an allowed row was refused: {:?}",
            by_id["aaaa0001"]
        );
        assert!(
            matches!(by_id["aaaa0002"], Outcome::RepoDenied(_)),
            "a config-added provider published a repo not on the allowlist: {:?}",
            by_id["aaaa0002"]
        );
        assert!(
            matches!(by_id["aaaa0003"], Outcome::PathDenied),
            "a config-added provider published from outside allow.paths: {:?}",
            by_id["aaaa0003"]
        );
    }

    #[test]
    fn one_id_shared_by_two_tools_is_two_rows_not_one() {
        // Both a uuid prefix and a hash can collide across tools. Rarely, but
        // fusing two unrelated agents into one row is silent when it happens,
        // and the surviving row would carry the other's branch and state.
        let mut m = machine("id-collision");
        let example_repo = m.checkout("work/example-repo", "example-org/example-repo", "main");

        let mut codex = candidate("d0d0d0d0", &example_repo, "2026-08-24T11:55:00Z");
        codex.tool = wire::ToolId::try_from("codex").unwrap();
        let mut qwen = candidate("d0d0d0d0", &example_repo, "2026-08-24T11:56:00Z");
        qwen.tool = wire::ToolId::try_from("qwen").unwrap();
        let mut twin = candidate("d0d0d0d0", &example_repo, "2026-08-24T11:57:00Z");
        twin.tool = wire::ToolId::try_from("codex").unwrap();

        let providers: Vec<Box<dyn Provider>> = vec![Box::new(Fake {
            id: ProviderId::generic("mixed"),
            priority: Priority::GENERIC,
            sessions: Vec::new(),
            candidates: vec![codex, qwen, twin],
        })];

        let published: Vec<_> = examine_with(
            &providers,
            &m.gate(&["work"], &["example-org/example-repo"]),
            &m,
            &options(),
            &procs(&[]),
            fixture_now(),
        )
        .into_iter()
        .filter_map(|d| match d.outcome {
            Outcome::Published(a) => Some(*a),
            _ => None,
        })
        .collect();

        // Two tools, three candidates: the two codex rows fold together
        // because they agree on the tool, and qwen stays its own row.
        assert_eq!(published.len(), 2, "{published:?}");
        let tools: Vec<String> = published
            .iter()
            .filter_map(|a| a.tool.as_ref().map(ToString::to_string))
            .collect();
        assert_eq!(tools, vec!["codex".to_owned(), "qwen".to_owned()]);
    }

    #[test]
    fn an_empty_allowlist_publishes_nothing() {
        let m = machine("empty");
        let gate = m.gate(&["work"], &[]);
        let decisions = examine_with(
            &m.providers(&options()),
            &gate,
            &m,
            &options(),
            &procs(&[64653]),
            fixture_now(),
        );
        assert!(
            decisions
                .iter()
                .all(|d| !matches!(d.outcome, Outcome::Published(_))),
            "{decisions:#?}"
        );
    }
}
