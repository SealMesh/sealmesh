use std::{collections::BTreeMap, ffi::OsStr, fmt::Write as _};

use jiff::Timestamp;
use sealmesh_proto::ProjectEntry;

use crate::{
    enrich::github::{CheckState, PrState, PrStatus},
    wire::{Agent, ProjectId, RunState, Snapshot},
};

pub type PrStatuses = BTreeMap<String, PrStatus>;

const MAX_FIELD: usize = 120;

const BAR_CELLS: usize = 16;

/// Whether the environment permits styling at all, whichever stream is being
/// written to.
///
/// One source for two surfaces: the report's own dim codes and the progress
/// line. Someone who sets `NO_COLOR` and still gets `ESC[2m` in their report
/// has been half-obeyed, which is worse than not honouring it at all — they
/// would at least have known.
pub fn styling_allowed() -> bool {
    styling_allowed_in(
        std::env::var_os("NO_COLOR").as_deref(),
        std::env::var_os("TERM").as_deref(),
    )
}

pub(crate) fn styling_allowed_in(no_color: Option<&OsStr>, term: Option<&OsStr>) -> bool {
    // The NO_COLOR convention is presence, not value — but an empty value
    // does not count, so `NO_COLOR=` reads as unset.
    if no_color.is_some_and(|value| !value.is_empty()) {
        return false;
    }
    term != Some(OsStr::new("dumb"))
}

pub fn sanitize(value: &str, max: usize) -> String {
    value
        .chars()
        .filter(|c| !c.is_control() && !matches!(c, '\u{7f}'..='\u{9f}') && !is_format_char(*c))
        .take(max)
        .collect()
}

/// The chat wire's own table: bidi, zero-width and other format characters,
/// the Unicode tag block, and the line separators. One table for a chat body
/// and a terminal line, so what `room_read` hands a model and what `chat`
/// draws for a person cannot disagree.
fn is_format_char(c: char) -> bool {
    sealmesh_chat_wire::is_hidden(c)
}

pub enum HostStatus<'a> {
    Answering(&'a Snapshot),
    NotSharing { host: String },
    Unreachable { host: String },
    Throttled { host: String },
    Unreadable { host: String, reason: String },
}

/// One host's agents that belong in a given section: the host's name, the
/// agents themselves (borrowed from its `Snapshot`), and when that snapshot
/// was collected.
type SectionEntry<'a> = (String, Vec<&'a Agent>, Timestamp);

/// Groups rendered rows by project, using the mapping the router holds. This
/// changes the top-level shape of the output: a project section (sorted by
/// id) holds every agent, from every host, whose repo is in that project, so
/// two repos of one project land together even when they run on different
/// machines. A repo the mapping does not place goes under an explicit
/// `ungrouped` heading — a repo in no project is a fact worth showing, not an
/// absence to hide. `NotSharing`, `Unreachable`, `Throttled` and `Unreadable` rows carry no
/// repo at all, so they get their own `not answering` section rather than
/// being folded into either of the above.
pub fn render(
    hosts: &[HostStatus<'_>],
    projects: &[ProjectEntry],
    prs: &PrStatuses,
    now: Timestamp,
    color: bool,
) -> String {
    let mut out = String::new();
    if hosts.is_empty() {
        out.push_str("No machines are connected. Is a daemon running?\n");
        return out;
    }

    // A router's Projects reply is hostile input, not a peer we trust to have
    // already validated it (rule 21): a repo the reply lists under two
    // entries — whether twice in one project or split across two — is
    // refused whole, the same way the server's own `ProjectMap::parse`
    // refuses to load part of a conflicting map. Silently letting the later
    // entry win would resolve an ambiguity the router itself never resolved.
    let by_repo = project_lookup(projects).unwrap_or_default();
    let mut grouped: BTreeMap<ProjectId, Vec<SectionEntry<'_>>> = BTreeMap::new();
    let mut ungrouped: Vec<SectionEntry<'_>> = Vec::new();
    let mut not_answering: Vec<&HostStatus<'_>> = Vec::new();

    for host in hosts {
        let HostStatus::Answering(snapshot) = host else {
            not_answering.push(host);
            continue;
        };
        if snapshot.agents.is_empty() {
            // No agents means no repo, which trivially places this host in no
            // project — the same "nothing to group by" reason a repo outside
            // the mapping lands here.
            ungrouped.push((
                snapshot.host.as_str().to_owned(),
                Vec::new(),
                snapshot.collected_at,
            ));
            continue;
        }
        let mut by_project: BTreeMap<Option<ProjectId>, Vec<&Agent>> = BTreeMap::new();
        for agent in &snapshot.agents {
            let key = by_repo
                .get(&agent.repo.as_str().to_ascii_lowercase())
                .cloned();
            by_project.entry(key).or_default().push(agent);
        }
        for (key, agents) in by_project {
            let entry = (
                snapshot.host.as_str().to_owned(),
                agents,
                snapshot.collected_at,
            );
            match key {
                Some(project) => grouped.entry(project).or_default().push(entry),
                None => ungrouped.push(entry),
            }
        }
    }

    for (project, entries) in &grouped {
        let _ = writeln!(out, "== {project} ==");
        for (host, agents, collected_at) in entries {
            render_agent_group(&mut out, host, agents, *collected_at, now, prs);
        }
    }

    if !ungrouped.is_empty() {
        let _ = writeln!(out, "== ungrouped ==");
        for (host, agents, collected_at) in &ungrouped {
            render_agent_group(&mut out, host, agents, *collected_at, now, prs);
        }
    }

    if !not_answering.is_empty() {
        let _ = writeln!(out, "== not answering ==");
        for host in not_answering {
            render_host(&mut out, host, prs, now, color);
        }
    }

    out
}

/// Printed after the report when the router withheld part of the answer.
///
/// It says the answer is partial and why that can happen, and nothing about
/// what was withheld: no count, no repo, no machine (rule 25). The causes are
/// listed together on purpose — which one applied is exactly the detail the
/// router does not send. Without this, a narrowed answer looks like an empty
/// fleet, which is how a real outage once passed for a config error.
pub const PARTIAL_NOTICE: &str = "\
note: this answer is partial. Work in a repo your GitHub login cannot read was
      left out, and so was work in a repo whose access could not be confirmed
      just now (GitHub unreachable, or a repo renamed on GitHub). A machine
      shown with nothing running may be running work you cannot see.
";

/// The report, followed by [`PARTIAL_NOTICE`] when the answer was partial.
/// Never the notice without the rows, and never the narrowed rows without the
/// notice.
pub fn render_answer(
    hosts: &[HostStatus<'_>],
    projects: &[ProjectEntry],
    prs: &PrStatuses,
    now: Timestamp,
    color: bool,
    partial: bool,
) -> String {
    let mut out = render(hosts, projects, prs, now, color);
    if partial {
        out.push_str(PARTIAL_NOTICE);
    }
    out
}

/// Case-insensitive, same as the router's own `ProjectMap`: GitHub treats a
/// repo name case-insensitively, and the client re-deriving that rule
/// independently would be one more place for it to drift from the server's.
///
/// `None` means the reply is self-contradictory — the same repo key placed
/// under more than one entry — which a well-behaved router's own
/// `ProjectMap::parse` can never produce (it refuses to load a conflicting
/// map at the source), but a reply is hostile input regardless of what
/// produced it (rule 45: validate at the boundary, not by trusting the
/// peer already did).
fn project_lookup(projects: &[ProjectEntry]) -> Option<BTreeMap<String, ProjectId>> {
    let mut map = BTreeMap::new();
    for entry in projects {
        for repo in &entry.repos {
            let key = repo.as_str().to_ascii_lowercase();
            if map.insert(key, entry.id.clone()).is_some() {
                return None;
            }
        }
    }
    Some(map)
}

fn render_agent_group(
    out: &mut String,
    host: &str,
    agents: &[&Agent],
    collected_at: Timestamp,
    now: Timestamp,
    prs: &PrStatuses,
) {
    let _ = writeln!(
        out,
        "{} — {} {}, read {} ago",
        sanitize(host, MAX_FIELD),
        agents.len(),
        if agents.len() == 1 { "agent" } else { "agents" },
        ago(collected_at, now)
    );
    if agents.is_empty() {
        let _ = writeln!(out, "  (sharing is on, nothing running)");
    }
    for agent in agents {
        render_agent(out, agent, prs, now);
    }
    out.push('\n');
}

fn render_host(
    out: &mut String,
    host: &HostStatus<'_>,
    prs: &PrStatuses,
    now: Timestamp,
    color: bool,
) {
    let dim = if color { "\u{1b}[2m" } else { "" };
    let reset = if color { "\u{1b}[0m" } else { "" };

    match host {
        HostStatus::Unreadable { host, reason } => {
            let _ = writeln!(
                out,
                "{} — unreadable: {}\n",
                sanitize(host, MAX_FIELD),
                sanitize(reason, MAX_FIELD)
            );
        }
        HostStatus::Unreachable { host } => {
            let _ = writeln!(
                out,
                "{dim}{} — unreachable (no daemon connected){reset}\n",
                sanitize(host, MAX_FIELD)
            );
        }
        HostStatus::NotSharing { host } => {
            let _ = writeln!(
                out,
                "{dim}{} — not sharing (off switch on){reset}\n",
                sanitize(host, MAX_FIELD)
            );
        }
        // Up and sharing, but its daemon refused this query because it has
        // been asked more often than it answers. Not dimmed like a machine
        // that is gone: this one is here, and will answer again shortly.
        HostStatus::Throttled { host } => {
            let _ = writeln!(
                out,
                "{} — throttled (connected, but asked too often; try again shortly)\n",
                sanitize(host, MAX_FIELD)
            );
        }
        HostStatus::Answering(snapshot) => {
            let _ = writeln!(
                out,
                "{} — {} {}, read {} ago",
                sanitize(snapshot.host.as_str(), MAX_FIELD),
                snapshot.agents.len(),
                if snapshot.agents.len() == 1 {
                    "agent"
                } else {
                    "agents"
                },
                ago(snapshot.collected_at, now)
            );
            if snapshot.agents.is_empty() {
                let _ = writeln!(out, "  (sharing is on, nothing running)");
            }
            for agent in &snapshot.agents {
                render_agent(out, agent, prs, now);
            }
            out.push('\n');
        }
    }
}

fn render_agent(out: &mut String, agent: &Agent, prs: &PrStatuses, now: Timestamp) {
    let identity = match (&agent.ticket, &agent.branch) {
        (Some(ticket), Some(branch)) => {
            format!("{ticket}  {}", sanitize(branch.as_str(), MAX_FIELD))
        }
        (Some(ticket), None) => ticket.to_string(),
        (None, Some(branch)) => sanitize(branch.as_str(), MAX_FIELD),
        (None, None) => "(no branch)".to_owned(),
    };
    let identity = format!("{}  {identity}", sanitize(agent.repo.as_str(), MAX_FIELD));

    // The tool goes on the header, with identity, not in the facts line,
    // which is activity. `None` renders as nothing at all: during a rollout an
    // old daemon's rows should look exactly like they did yesterday rather
    // than growing a wall of `unknown`.
    let tool = agent
        .tool
        .as_ref()
        .map(|t| format!(" · {}", sanitize(t.as_str(), MAX_FIELD)))
        .unwrap_or_default();

    let header = format!(
        "{}  {} · {}{tool}",
        agent.id,
        run_state_label(agent.run_state),
        ago(agent.started_at, now)
    );

    let mut facts: Vec<String> = Vec::new();
    if let Some(ahead) = agent.ahead
        && ahead > 0
    {
        facts.push(format!("↑{ahead}"));
    }
    if let Some(dirty) = agent.dirty_files
        && dirty > 0
    {
        facts.push(format!("{dirty} dirty"));
    }
    match agent.tasks {
        Some(tasks) => facts.push(format!(
            "tasks {} {}/{}",
            bar(usize::from(tasks.completed()), usize::from(tasks.total())),
            tasks.completed(),
            tasks.total()
        )),
        None => facts.push("tasks —".to_owned()),
    }
    if let Some(pr) = agent.pr {
        let status = prs
            .get(&crate::enrich::github::cache_key(&agent.repo, pr))
            .copied();
        facts.push(pr_label(pr, status));
    }

    let _ = writeln!(out, "  {header}");
    let _ = writeln!(out, "    {identity}");
    let _ = writeln!(out, "    {}", facts.join(" · "));
}

fn pr_label(number: u32, status: Option<PrStatus>) -> String {
    let Some(status) = status else {
        return format!("PR #{number} · CI ?");
    };
    match status.state {
        PrState::Merged => format!("PR #{number} · merged"),
        PrState::Closed => format!("PR #{number} · closed"),
        PrState::Draft => format!("PR #{number} draft · {}", check_label(status.checks)),
        PrState::Open => format!("PR #{number} · {}", check_label(status.checks)),
    }
}

fn check_label(checks: CheckState) -> &'static str {
    match checks {
        CheckState::Green => "CI green",
        CheckState::Red => "CI RED",
        CheckState::Running => "CI running",
        CheckState::None => "no CI",
        CheckState::Unknown => "CI ?",
    }
}

fn run_state_label(state: RunState) -> &'static str {
    match state {
        RunState::Working => "working",
        RunState::Idle => "idle",
        RunState::NeedsInput => "NEEDS INPUT",
        RunState::Done => "done",
        RunState::Unknown => "unknown",
    }
}

/// The one progress bar this binary has. `progress` renders pull request
/// lookups through it too, so a `who` never shows two different vocabularies
/// for the same idea.
pub(crate) fn bar(completed: usize, total: usize) -> String {
    if total == 0 {
        return "─".repeat(BAR_CELLS);
    }
    let filled = (completed.saturating_mul(BAR_CELLS) / total).min(BAR_CELLS);
    format!("{}{}", "█".repeat(filled), "░".repeat(BAR_CELLS - filled))
}

pub fn ago(then: Timestamp, now: Timestamp) -> String {
    let seconds = (now - then).get_seconds();
    if seconds < 0 {
        return "clock skew".to_owned();
    }
    match seconds {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m", s / 60),
        s if s < 86_400 => format!("{}h{}m", s / 3600, (s % 3600) / 60),
        s => format!("{}d", s / 86_400),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::wire::{
        BranchName, HostId, JobId, RepoSlug, SCHEMA_VERSION, TaskProgress, TicketId,
    };

    #[test]
    fn no_color_silences_every_styled_surface_from_one_source() {
        // `progress::decide` reads this same function, so a reader who sets
        // NO_COLOR cannot lose the spinner and keep the report's dim codes.
        assert!(styling_allowed_in(None, None));
        assert!(styling_allowed_in(
            Some(OsStr::new("")),
            Some(OsStr::new("xterm"))
        ));
        assert!(!styling_allowed_in(Some(OsStr::new("1")), None));
        assert!(!styling_allowed_in(None, Some(OsStr::new("dumb"))));
    }

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    #[test]
    fn a_line_cannot_be_reversed_or_hollowed_out_by_invisible_characters() {
        for hostile in [
            "santi\u{202e}ogaitnas",
            "santi\u{2066}ago\u{2069}",
            "santi\u{200b}ago",
            "santi\u{feff}ago",
            "santi\u{00ad}ago",
            "santi\u{e0020}\u{e0069}\u{e007f}ago",
            "santi\u{e0001}ago",
            "santi\u{2028}ago",
        ] {
            let out = sanitize(hostile, 64);
            assert!(
                out.chars().all(|c| c.is_ascii_alphanumeric()),
                "{hostile:?} sanitised to {out:?}"
            );
        }
        assert_eq!(sanitize("sandbank", 64), "sandbank");
    }

    #[test]
    fn every_unicode_tag_is_stripped_from_a_terminal_line() {
        let tags: String = (0xe0000..=0xe007f_u32)
            .map(|code| char::from_u32(code).unwrap())
            .collect();
        assert_eq!(sanitize(&format!("a{tags}b"), 256), "ab");
    }

    fn rendered(status: HostStatus<'_>) -> String {
        render(
            &[status],
            &[],
            &PrStatuses::new(),
            ts("2026-08-10T19:57:01.322Z"),
            false,
        )
    }

    fn project(id: &str, repos: &[&str]) -> ProjectEntry {
        ProjectEntry {
            id: ProjectId::try_from(id).unwrap(),
            repos: repos
                .iter()
                .map(|r| RepoSlug::try_from(*r).unwrap())
                .collect(),
        }
    }

    #[test]
    fn two_agents_on_the_same_branch_in_different_repos_are_distinguishable() {
        let mut a = agent("aaaa1111");
        a.repo = RepoSlug::try_from("example-org/sealmesh").unwrap();
        a.branch = Some(BranchName::try_from("main").unwrap());
        a.ticket = None;

        let mut b = agent("bbbb2222");
        b.repo = RepoSlug::try_from("example-org/example-repo").unwrap();
        b.branch = Some(BranchName::try_from("main").unwrap());
        b.ticket = None;

        let mut snapshot = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
        snapshot.agents = vec![a, b];
        let out = rendered(HostStatus::Answering(&snapshot));

        assert!(out.contains("example-org/sealmesh  main"), "{out}");
        assert!(out.contains("example-org/example-repo  main"), "{out}");

        let rows: Vec<&str> = out.lines().filter(|l| l.contains("main")).collect();
        assert_eq!(rows.len(), 2, "{out}");
        assert_ne!(
            rows.first(),
            rows.last(),
            "two repos rendered as identical rows"
        );
    }

    #[test]
    fn the_repo_owner_is_never_abbreviated_away() {
        let mut a = agent("aaaa1111");
        a.repo = RepoSlug::try_from("someone-else/example-repo").unwrap();

        let mut snapshot = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
        snapshot.agents = vec![a];
        let out = rendered(HostStatus::Answering(&snapshot));

        assert!(out.contains("someone-else/example-repo"), "{out}");
    }

    #[test]
    fn the_repo_is_present_whatever_else_the_row_has() {
        for (ticket, branch) in [
            (
                Some(TicketId::new("ENG", 557).unwrap()),
                Some(BranchName::try_from("dev").unwrap()),
            ),
            (Some(TicketId::new("ENG", 557).unwrap()), None),
            (None, Some(BranchName::try_from("dev").unwrap())),
            (None, None),
        ] {
            let mut a = agent("aaaa1111");
            a.ticket = ticket.clone();
            a.branch = branch.clone();

            let mut snapshot = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
            snapshot.agents = vec![a];
            let out = rendered(HostStatus::Answering(&snapshot));

            assert!(
                out.contains("example-org/example-repo"),
                "repo missing for ticket={ticket:?} branch={branch:?}: {out}"
            );
        }
    }

    fn agent(id: &str) -> Agent {
        Agent {
            id: JobId::try_from(id).unwrap(),
            repo: RepoSlug::try_from("example-org/example-repo").unwrap(),
            run_state: RunState::NeedsInput,
            branch: Some(BranchName::try_from("feature/eng-557-qa-rescore-ui").unwrap()),
            ticket: Some(TicketId::new("ENG", 557).unwrap()),
            ahead: Some(3),
            dirty_files: Some(7),
            tasks: Some(TaskProgress::new(9, 13).unwrap()),
            pr: Some(627),
            tool: None,
            started_at: ts("2026-08-10T19:44:51.322Z"),
            updated_at: ts("2026-08-10T19:56:51.322Z"),
        }
    }

    fn snapshot(host: &str, collected: &str) -> Snapshot {
        Snapshot {
            schema: SCHEMA_VERSION,
            host: HostId::try_from(host).unwrap(),
            collected_at: ts(collected),
            agents: vec![agent("1e631e34")],
        }
    }

    #[test]
    fn a_terminal_escape_never_reaches_the_terminal() {
        let hostile = "feature/\u{1b}]0;pwn\u{7}x\u{9b}31m";
        let clean = sanitize(hostile, MAX_FIELD);
        assert!(!clean.contains('\u{1b}'), "{clean:?}");
        assert!(!clean.contains('\u{7}'), "{clean:?}");
        assert!(!clean.contains('\u{9b}'), "{clean:?}");
        assert_eq!(clean, "feature/]0;pwnx31m");
    }

    #[test]
    fn a_long_value_is_capped_on_char_boundaries() {
        let long = "é".repeat(500);
        let clean = sanitize(&long, MAX_FIELD);
        assert_eq!(clean.chars().count(), MAX_FIELD);
    }

    #[test]
    fn unreachable_not_sharing_and_unreadable_are_never_the_same_row() {
        let unreachable = rendered(HostStatus::Unreachable {
            host: "shelter".to_owned(),
        });
        let not_sharing = rendered(HostStatus::NotSharing {
            host: "shelter".to_owned(),
        });
        let unreadable = rendered(HostStatus::Unreadable {
            host: "shelter".to_owned(),
            reason: "bad json".to_owned(),
        });

        assert!(unreachable.contains("unreachable"), "{unreachable}");
        assert!(not_sharing.contains("not sharing"), "{not_sharing}");
        assert!(unreadable.contains("unreadable"), "{unreadable}");
        assert_ne!(unreachable, not_sharing);
        assert_ne!(not_sharing, unreadable);
    }

    #[test]
    fn a_throttled_host_reads_as_neither_gone_nor_opted_out_nor_answering() {
        let throttled = rendered(HostStatus::Throttled {
            host: "shelter".to_owned(),
        });
        assert!(throttled.contains("== not answering =="), "{throttled}");
        assert!(throttled.contains("shelter — throttled"), "{throttled}");
        for other in ["unreachable", "not sharing", "unreadable", "agent", "read "] {
            assert!(!throttled.contains(other), "`{other}` in {throttled}");
        }
    }

    #[test]
    fn an_answering_host_reports_when_it_read_its_own_files() {
        let snapshot = snapshot("shelter", "2026-08-10T19:56:51.322Z");
        let out = render(
            &[HostStatus::Answering(&snapshot)],
            &[],
            &PrStatuses::new(),
            ts("2026-08-10T19:57:01.322Z"),
            false,
        );
        assert!(out.contains("read 10s ago"), "{out}");
        assert!(!out.contains("stale"), "{out}");
    }

    #[test]
    fn an_unreadable_host_is_named_rather_than_dropped() {
        let good = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
        let out = render(
            &[
                HostStatus::Answering(&good),
                HostStatus::Unreadable {
                    host: "shelter".to_owned(),
                    reason: "snapshot declares schema 99, which this build does not read"
                        .to_owned(),
                },
            ],
            &[],
            &PrStatuses::new(),
            ts("2026-08-10T19:57:03.322Z"),
            false,
        );
        assert!(out.contains("sandbank — 1 agent"), "{out}");
        assert!(out.contains("shelter — unreadable"), "{out}");
    }

    #[test]
    fn no_task_list_renders_as_no_data_not_as_no_progress() {
        let mut snapshot = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
        snapshot.agents.first_mut().unwrap().tasks = None;
        let out = render(
            &[HostStatus::Answering(&snapshot)],
            &[],
            &PrStatuses::new(),
            ts("2026-08-10T19:57:03.322Z"),
            false,
        );
        assert!(out.contains("tasks —"), "{out}");
        assert!(!out.contains("0/0"), "{out}");
        assert!(!out.contains('░'), "{out}");
    }

    #[test]
    fn an_off_beacon_reads_differently_from_a_missing_one() {
        let mut snapshot = snapshot("shelter", "2026-08-10T19:56:51.322Z");
        snapshot.agents.clear();
        let out = render(
            &[HostStatus::Answering(&snapshot)],
            &[],
            &PrStatuses::new(),
            ts("2026-08-10T19:57:03.322Z"),
            false,
        );
        assert!(out.contains("sharing is on, nothing running"), "{out}");
    }

    #[test]
    fn a_future_timestamp_is_called_skew_not_rendered_as_negative() {
        assert_eq!(
            ago(ts("2026-08-10T20:00:00Z"), ts("2026-08-10T19:00:00Z")),
            "clock skew"
        );
        assert_eq!(
            ago(ts("2026-08-10T19:00:00Z"), ts("2026-08-10T19:00:14Z")),
            "14s"
        );
        assert_eq!(
            ago(ts("2026-08-10T19:00:00Z"), ts("2026-08-10T19:14:00Z")),
            "14m"
        );
        assert_eq!(
            ago(ts("2026-08-10T14:00:00Z"), ts("2026-08-10T19:24:00Z")),
            "5h24m"
        );
    }

    #[test]
    fn the_progress_bar_matches_the_counts() {
        assert_eq!(bar(0, 13), "░".repeat(BAR_CELLS));
        assert_eq!(bar(13, 13), "█".repeat(BAR_CELLS));
        assert_eq!(bar(9, 13).chars().count(), BAR_CELLS);
        assert_eq!(bar(5, 0).chars().count(), BAR_CELLS);
    }

    #[test]
    fn agents_in_two_repos_of_one_project_are_grouped_together_even_on_different_hosts() {
        let mut a = agent("aaaa1111");
        a.repo = RepoSlug::try_from("example-org/example-repo").unwrap();
        let mut sandbank = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
        sandbank.agents = vec![a];

        let mut b = agent("bbbb2222");
        b.repo = RepoSlug::try_from("example-org/example-repo-portal").unwrap();
        let mut shelter = snapshot("shelter", "2026-08-10T19:56:51.322Z");
        shelter.agents = vec![b];

        let projects = [project(
            "example-repo",
            &[
                "example-org/example-repo",
                "example-org/example-repo-portal",
            ],
        )];
        let out = render(
            &[
                HostStatus::Answering(&sandbank),
                HostStatus::Answering(&shelter),
            ],
            &projects,
            &PrStatuses::new(),
            ts("2026-08-10T19:57:01.322Z"),
            false,
        );

        assert!(out.contains("== example-repo =="), "{out}");
        let heading = out.find("== example-repo ==").unwrap();
        let after_heading = heading + "== example-repo ==".len();
        let next_heading = out[after_heading..]
            .find("==")
            .map(|i| i + after_heading)
            .unwrap_or(out.len());
        let section = &out[heading..next_heading];
        assert!(section.contains("sandbank"), "{section}");
        assert!(section.contains("shelter"), "{section}");
        assert!(section.contains("example-org/example-repo"), "{section}");
        assert!(
            section.contains("example-org/example-repo-portal"),
            "{section}"
        );
        assert!(!out.contains("== ungrouped =="), "{out}");
    }

    #[test]
    fn a_repo_in_no_project_renders_in_an_explicit_ungrouped_section() {
        let mut in_project = agent("aaaa1111");
        in_project.repo = RepoSlug::try_from("example-org/example-repo").unwrap();
        let mut outside = agent("bbbb2222");
        outside.repo = RepoSlug::try_from("example-org/sealmesh").unwrap();

        let mut snap = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
        snap.agents = vec![in_project, outside];

        let projects = [project("example-repo", &["example-org/example-repo"])];
        let out = render(
            &[HostStatus::Answering(&snap)],
            &projects,
            &PrStatuses::new(),
            ts("2026-08-10T19:57:01.322Z"),
            false,
        );

        assert!(out.contains("== example-repo =="), "{out}");
        assert!(out.contains("== ungrouped =="), "{out}");
        let ungrouped_at = out.find("== ungrouped ==").unwrap();
        assert!(
            out[ungrouped_at..].contains("example-org/sealmesh"),
            "{out}"
        );
        let example_repo_at = out.find("== example-repo ==").unwrap();
        let example_repo_section = &out[example_repo_at..ungrouped_at];
        assert!(
            !example_repo_section.contains("example-org/sealmesh"),
            "the ungrouped repo leaked into the project section: {out}"
        );
    }

    #[test]
    fn not_sharing_and_unreachable_hosts_render_in_their_own_section_never_dropped() {
        let mut snap = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
        snap.agents = vec![agent("aaaa1111")];
        let projects = [project("example-repo", &["example-org/example-repo"])];

        let out = render(
            &[
                HostStatus::Answering(&snap),
                HostStatus::NotSharing {
                    host: "shelter".to_owned(),
                },
                HostStatus::Unreachable {
                    host: "prism".to_owned(),
                },
            ],
            &projects,
            &PrStatuses::new(),
            ts("2026-08-10T19:57:01.322Z"),
            false,
        );

        assert!(out.contains("== not answering =="), "{out}");
        let not_answering_at = out.find("== not answering ==").unwrap();
        let section = &out[not_answering_at..];
        assert!(section.contains("shelter — not sharing"), "{out}");
        assert!(section.contains("prism — unreachable"), "{out}");
        // Neither carries a repo, so neither can have landed in a project or
        // the ungrouped section instead.
        let before = &out[..not_answering_at];
        assert!(!before.contains("shelter"), "{out}");
        assert!(!before.contains("prism"), "{out}");
    }

    #[test]
    fn an_old_router_with_no_project_mapping_renders_everything_ungrouped() {
        let mut snap = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
        snap.agents = vec![agent("aaaa1111")];

        // An empty mapping is what the reader falls back to when an old
        // router never answers `ClientFrame::Projects` at all.
        let out = render(
            &[HostStatus::Answering(&snap)],
            &[],
            &PrStatuses::new(),
            ts("2026-08-10T19:57:01.322Z"),
            false,
        );

        assert!(out.contains("== ungrouped =="), "{out}");
        assert!(out.contains("example-org/example-repo"), "{out}");
    }

    #[test]
    fn attribution_refusal_still_renders_distinctly_inside_not_answering() {
        // Rule 21: a snapshot claiming a different host than the router
        // attributed it to is refused and rendered `Unreadable`, and that must
        // survive grouping unchanged — it still cannot be confused with a
        // clean `Unreachable` or `NotSharing` row, and it still cannot be
        // silently dropped by the new section logic.
        let out = rendered(HostStatus::Unreadable {
            host: "shelter".to_owned(),
            reason: "answer claims to be a different host".to_owned(),
        });
        assert!(out.contains("== not answering =="), "{out}");
        assert!(
            out.contains("shelter — unreadable: answer claims to be a different host"),
            "{out}"
        );
    }

    #[test]
    fn grouping_ignores_the_case_github_ignores() {
        // The router's own `ProjectMap` folds a repo's case before matching
        // (GitHub treats repo names case-insensitively), and the client side
        // has to make the same call independently — a mismatch here would
        // silently drop an agent into `ungrouped` whenever an agent's
        // self-reported repo casing differs from what an operator typed into
        // `SEALMESH_PROJECTS`.
        let mut a = agent("aaaa1111");
        a.repo = RepoSlug::try_from("example-org/Example-repo").unwrap();
        let mut snap = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
        snap.agents = vec![a];

        let projects = [project("example-repo", &["example-org/example-repo"])];
        let out = render(
            &[HostStatus::Answering(&snap)],
            &projects,
            &PrStatuses::new(),
            ts("2026-08-10T19:57:01.322Z"),
            false,
        );

        assert!(out.contains("== example-repo =="), "{out}");
        assert!(!out.contains("== ungrouped =="), "{out}");
    }

    #[test]
    fn one_host_with_agents_in_two_different_projects_appears_under_both() {
        let mut a = agent("aaaa1111");
        a.repo = RepoSlug::try_from("example-org/example-repo").unwrap();
        let mut b = agent("bbbb2222");
        b.repo = RepoSlug::try_from("example-org/example-tools").unwrap();

        let mut snap = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
        snap.agents = vec![a, b];

        let projects = [
            project("example-repo", &["example-org/example-repo"]),
            project("example-tools", &["example-org/example-tools"]),
        ];
        let out = render(
            &[HostStatus::Answering(&snap)],
            &projects,
            &PrStatuses::new(),
            ts("2026-08-10T19:57:01.322Z"),
            false,
        );

        assert!(out.contains("== example-repo =="), "{out}");
        assert!(out.contains("== example-tools =="), "{out}");
        assert!(!out.contains("== ungrouped =="), "{out}");

        let example_repo_at = out.find("== example-repo ==").unwrap();
        let example_tools_at = out.find("== example-tools ==").unwrap();
        let (first, first_repo, second, second_repo) = if example_repo_at < example_tools_at {
            (
                example_repo_at,
                "example-org/example-repo",
                example_tools_at,
                "example-org/example-tools",
            )
        } else {
            (
                example_tools_at,
                "example-org/example-tools",
                example_repo_at,
                "example-org/example-repo",
            )
        };
        let first_section = &out[first..second];
        assert!(first_section.contains("sandbank"), "{out}");
        assert!(first_section.contains(first_repo), "{out}");
        assert!(!first_section.contains(second_repo), "{out}");
    }

    #[test]
    fn a_repo_listed_under_two_projects_refuses_the_whole_mapping_not_the_later_entry() {
        // A well-behaved router's own `ProjectMap::parse` can never produce
        // this reply -- it refuses to load a conflicting map at the source.
        // But the reply is hostile input regardless of what produced it
        // (rule 21), so the reader must not resolve the ambiguity by letting
        // whichever entry it saw last win; it must refuse the mapping and
        // render everything ungrouped, same as an old router that never
        // replied at all.
        let mut a = agent("aaaa1111");
        a.repo = RepoSlug::try_from("example-org/example-repo").unwrap();
        let mut snap = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
        snap.agents = vec![a];

        let projects = [
            project("example-repo", &["example-org/example-repo"]),
            project("example-tools", &["example-org/example-repo"]),
        ];
        let out = render(
            &[HostStatus::Answering(&snap)],
            &projects,
            &PrStatuses::new(),
            ts("2026-08-10T19:57:01.322Z"),
            false,
        );

        assert!(!out.contains("== example-repo =="), "{out}");
        assert!(!out.contains("== example-tools =="), "{out}");
        assert!(out.contains("== ungrouped =="), "{out}");
        assert!(out.contains("example-org/example-repo"), "{out}");
    }

    #[test]
    fn a_partial_answer_renders_its_notice_after_the_rows() {
        let mut idle = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
        idle.agents.clear();
        let out = render_answer(
            &[HostStatus::Answering(&idle)],
            &[],
            &PrStatuses::new(),
            ts("2026-08-10T19:57:01.322Z"),
            false,
            true,
        );

        let notice = out.find("this answer is partial").expect(&out);
        let row = out.find("sandbank — 0 agents").expect(&out);
        assert!(row < notice, "the notice must follow the rows: {out}");
        assert!(out.ends_with(PARTIAL_NOTICE), "{out}");
    }

    #[test]
    fn a_complete_answer_carries_no_partial_notice() {
        let snap = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
        let hosts = [HostStatus::Answering(&snap)];
        let now = ts("2026-08-10T19:57:01.322Z");
        let out = render_answer(&hosts, &[], &PrStatuses::new(), now, false, false);

        assert!(!out.contains("partial"), "{out}");
        assert_eq!(out, render(&hosts, &[], &PrStatuses::new(), now, false));
    }

    #[test]
    fn the_partial_notice_names_no_count_repo_or_machine() {
        // Rule 25. The router sends one bit, and the notice must not dress it
        // up as more: nothing numeric, nothing that looks like a slug.
        assert!(
            !PARTIAL_NOTICE.chars().any(|c| c.is_ascii_digit()),
            "{PARTIAL_NOTICE}"
        );
        assert!(!PARTIAL_NOTICE.contains('/'), "{PARTIAL_NOTICE}");
    }

    #[test]
    fn a_repo_listed_twice_in_one_project_also_refuses_the_mapping() {
        let mut a = agent("aaaa1111");
        a.repo = RepoSlug::try_from("example-org/example-repo").unwrap();
        let mut snap = snapshot("sandbank", "2026-08-10T19:56:51.322Z");
        snap.agents = vec![a];

        // Case-insensitive, same as the server: "Example-repo" and "example-repo" are
        // one repo, not two.
        let projects = [project(
            "example-repo",
            &["example-org/example-repo", "example-org/Example-repo"],
        )];
        let out = render(
            &[HostStatus::Answering(&snap)],
            &projects,
            &PrStatuses::new(),
            ts("2026-08-10T19:57:01.322Z"),
            false,
        );

        assert!(!out.contains("== example-repo =="), "{out}");
        assert!(out.contains("== ungrouped =="), "{out}");
    }
}
