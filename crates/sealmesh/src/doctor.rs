use std::collections::BTreeMap;

use anyhow::Result;

use crate::{
    collect::{self, Decision, Outcome, git::GitResolver, roots},
    config::Config,
    github_auth, paths, render,
};

pub async fn run(config: &Config) -> Result<()> {
    println!("router            {}", config.server.connect_url()?);
    println!("integration       origin/{}", config.integration_branch);
    println!("ticket prefixes   {}", config.ticket_prefixes.join(", "));
    println!("host              derived from your GitHub login at connect");

    let killed = crate::daemon::is_off();
    match Config::off_switch_path() {
        Some(path) if killed => println!(
            "off switch        ON — {} exists, so queries answer \"not sharing\"",
            path.display()
        ),
        Some(path) => println!("off switch        off ({} does not exist)", path.display()),
        None => println!("off switch        unavailable: no home directory"),
    }
    println!("chat              {}", chat_status_line(config, killed));
    let (project_rooms, hook_drain) = chat_option_lines(config);
    println!("project rooms     {project_rooms}");
    println!("hook drain        {hook_drain}");
    if let Some(path) = crate::audit::path() {
        let count = crate::audit::read(&path).map(|e| e.len()).unwrap_or(0);
        println!("audit log         {} ({count} answered)", path.display());
    }
    if let Some(dir) = paths::cache_home() {
        println!("cache             {}", dir.join("sealmesh").display());
    }

    println!("\nallowed repos");
    let gate = config.gate()?;
    for repo in gate.repos() {
        println!("  {repo}");
    }
    println!("allowed paths (canonicalised)");
    for path in gate.paths() {
        println!("  {}", path.display());
    }

    let roots = roots::discover_local(&config.claude.extra_roots);
    println!("\nclaude code roots ({} found)", roots.len());
    if roots.is_empty() {
        println!("  none. Expected at least ~/.claude with a jobs/ or sessions/ directory.");
    }
    for root in &roots {
        println!("  {}", root.path().display());
    }

    let procs = collect::liveness::ProcessTable::read();
    let options = config.collect_options();
    let providers = collect::providers::with_configured(
        roots.clone(),
        options.declared_dir.clone(),
        options.declared_ttl,
        &config.provider,
    );
    // The same set the collector uses, built the same way — this used to be a
    // hand-rolled duplicate of the collector's session read, and the two could
    // disagree about what was alive.
    declared_records(config, &collect::live_session_ids(&providers, &procs));

    let resolver = GitResolver::new(config.integration_branch.clone());
    let decisions = collect::examine(&providers, &gate, &resolver, &options);

    println!(
        "\nagents ({} seen{})",
        decisions.len(),
        by_source(&decisions)
    );
    if decisions.is_empty() {
        println!(
            "  none. Claude Code writes one file per background job under <root>/jobs/, \n  \
             and one per live session under <root>/sessions/."
        );
    }
    for decision in &decisions {
        let dir = render::sanitize(&decision.work_dir.to_string_lossy(), 200);
        let from = &decision.source;
        match &decision.outcome {
            Outcome::Published(agent) => {
                let branch = agent
                    .branch
                    .as_ref()
                    .map_or("(no branch)".to_owned(), ToString::to_string);
                println!(
                    "  {}  {from:<9}  PUBLISHED  {}  {}  {branch}",
                    decision.id, agent.repo, agent.run_state
                );
            }
            Outcome::PathDenied => {
                println!(
                    "  {}  {from:<9}  hidden     not under an allowed path: {dir}",
                    decision.id
                );
            }
            Outcome::NoRepo => {
                println!(
                    "  {}  {from:<9}  hidden     no git repo or no origin remote: {dir}",
                    decision.id
                );
            }
            Outcome::RepoDenied(slug) => {
                println!(
                    "  {}  {from:<9}  hidden     {slug} is not on allow.repos",
                    decision.id
                );
            }
        }
    }

    for warning in shared_checkout_warnings(&decisions) {
        println!("\n{warning}");
    }

    println!("\ngithub credential");
    match github_auth::token() {
        Ok(_) => println!("  resolved. Run `sealmesh daemon` to connect."),
        Err(error) => println!("  NOT resolved: {error:#}"),
    }

    Ok(())
}

/// The `doctor` chat line, as a pure function of the same decision
/// `chat_enabled` makes. Separated from `run`'s `println!` so a test can
/// drive all four `[chat] enabled` x kill-file combinations directly,
/// instead of asserting on stdout from an async function.
fn chat_status_line(config: &Config, killed: bool) -> &'static str {
    if config.chat_enabled(killed) {
        "enabled"
    } else if config.chat.enabled {
        "disabled (kill file overrides [chat] enabled)"
    } else {
        "disabled ([chat] enabled = false)"
    }
}

/// The two `[chat]` opt-ins, as `doctor` prints them. Each says its key, so
/// a reader knows what to change; neither matters while chat is off, and the
/// line above already says when it is.
fn chat_option_lines(config: &Config) -> (&'static str, &'static str) {
    let project_rooms = if config.chat.project_rooms {
        "on (resolving a checkout also names its project room)"
    } else {
        "off ([chat] project_rooms = false)"
    };
    let hook_drain = if config.chat.hook_drain {
        "on (the hook drains room messages into each turn)"
    } else {
        "off ([chat] hook_drain = false)"
    };
    (project_rooms, hook_drain)
}

fn declared_records(config: &Config, live: &std::collections::BTreeSet<String>) {
    let Some(dir) = paths::declared_agents_dir() else {
        return;
    };
    let ttl = config.declared.ttl();
    let reads = collect::declared::read_all(
        &dir,
        jiff::SignedDuration::try_from(ttl).unwrap_or(jiff::SignedDuration::from_secs(900)),
        jiff::Timestamp::now(),
        &collect::declared::LiveSessions(live),
    );

    println!(
        "\ndeclared records ({} in {}, ttl {}s)",
        reads.len(),
        dir.display(),
        ttl.as_secs()
    );
    if reads.is_empty() {
        println!("  none. See docs/contract.md for how a tool declares an agent.");
    }
    for read in &reads {
        let name = render::sanitize(&read.path.to_string_lossy(), 200);
        match &read.outcome {
            Ok(candidate) => println!("  {}  ok         {name}", candidate.id),
            Err(reason) => println!("  {:<8}  REFUSED    {name}\n            {reason}", ""),
        }
    }
}

fn by_source(decisions: &[Decision]) -> String {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for decision in decisions {
        *counts.entry(decision.source.to_string()).or_default() += 1;
    }
    if counts.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = counts
        .into_iter()
        .map(|(source, count)| format!("{count} {source}"))
        .collect();
    format!(": {}", parts.join(", "))
}

fn shared_checkout_warnings(decisions: &[Decision]) -> Vec<String> {
    let mut by_dir: BTreeMap<&std::path::Path, Vec<&Decision>> = BTreeMap::new();
    for decision in decisions {
        if matches!(decision.outcome, Outcome::Published(_)) {
            by_dir
                .entry(decision.work_dir.as_path())
                .or_default()
                .push(decision);
        }
    }

    by_dir
        .into_iter()
        .filter(|(_, sharing)| sharing.len() > 1)
        .map(|(dir, sharing)| {
            let ids: Vec<String> = sharing.iter().map(|d| d.id.to_string()).collect();
            format!(
                "! {} agents share {}\n    {}\n    They publish the same branch and ticket, so \
                 everyone reading sees identical rows.\n    Give each agent its own `git worktree`.",
                sharing.len(),
                render::sanitize(&dir.to_string_lossy(), 200),
                ids.join(", ")
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::path::PathBuf;

    use super::*;
    use crate::wire::{Agent, JobId, RepoSlug, RunState};

    fn decision(id: &str, dir: &str, published: bool) -> Decision {
        let agent = Agent {
            id: JobId::try_from(id).unwrap(),
            repo: RepoSlug::try_from("example-org/example-repo").unwrap(),
            run_state: RunState::Working,
            branch: None,
            ticket: None,
            ahead: None,
            dirty_files: None,
            tasks: None,
            pr: None,
            tool: None,
            started_at: "2026-08-13T12:00:00Z".parse().unwrap(),
            updated_at: "2026-08-13T12:00:00Z".parse().unwrap(),
        };
        Decision {
            id: JobId::try_from(id).unwrap(),
            source: collect::provider::ProviderId::CLAUDE_JOB,
            work_dir: PathBuf::from(dir),
            outcome: if published {
                Outcome::Published(Box::new(agent))
            } else {
                Outcome::PathDenied
            },
        }
    }

    #[test]
    fn the_header_tally_names_each_adapter() {
        let tally = by_source(&[
            decision("aaaa1111", "/w/a", true),
            decision("bbbb2222", "/w/b", true),
            session_decision("cccc3333", "/w/c"),
        ]);
        assert_eq!(tally, ": 2 job, 1 session");
        assert_eq!(by_source(&[]), "");
    }

    fn session_decision(id: &str, dir: &str) -> Decision {
        let mut d = decision(id, dir, true);
        d.source = collect::provider::ProviderId::CLAUDE_SESSION;
        d
    }

    #[test]
    fn two_agents_in_one_checkout_are_flagged() {
        let warnings = shared_checkout_warnings(&[
            decision("aaaa1111", "/w/example-repo", true),
            decision("bbbb2222", "/w/example-repo", true),
        ]);
        assert_eq!(warnings.len(), 1);
        let warning = warnings.first().unwrap();
        assert!(warning.contains("/w/example-repo"), "{warning}");
        assert!(warning.contains("worktree"), "{warning}");
        assert!(warning.contains("aaaa1111"), "{warning}");
    }

    #[test]
    fn agents_in_separate_worktrees_are_not_flagged() {
        let warnings = shared_checkout_warnings(&[
            decision("aaaa1111", "/w/example-repo-612", true),
            decision("bbbb2222", "/w/example-repo-557", true),
        ]);
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn agents_that_are_not_published_do_not_trigger_the_warning() {
        let warnings = shared_checkout_warnings(&[
            decision("aaaa1111", "/w/example-repo", false),
            decision("bbbb2222", "/w/example-repo", false),
        ]);
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn a_hostile_directory_name_cannot_steer_the_terminal_through_the_warning() {
        let warnings = shared_checkout_warnings(&[
            decision("aaaa1111", "/w/\u{1b}]0;pwn\u{7}", true),
            decision("bbbb2222", "/w/\u{1b}]0;pwn\u{7}", true),
        ]);
        let warning = warnings.first().unwrap();
        assert!(!warning.contains('\u{1b}'), "{warning:?}");
    }

    fn config_with_chat(enabled: bool) -> Config {
        let toml = format!(
            r#"
            ticket_prefixes = ["ENG"]
            [server]
            url = "https://router.example.com"
            [allow]
            repos = ["example-org/example-repo"]
            paths = ["/tmp"]
            [chat]
            enabled = {enabled}
        "#
        );
        toml::from_str(&toml).unwrap()
    }

    #[test]
    fn the_doctor_chat_line_covers_all_four_enabled_x_killed_combinations() {
        let off = config_with_chat(false);
        let on = config_with_chat(true);

        assert_eq!(
            chat_status_line(&off, false),
            "disabled ([chat] enabled = false)"
        );
        assert_eq!(
            chat_status_line(&off, true),
            "disabled ([chat] enabled = false)"
        );
        assert_eq!(chat_status_line(&on, false), "enabled");
        assert_eq!(
            chat_status_line(&on, true),
            "disabled (kill file overrides [chat] enabled)"
        );
    }

    #[test]
    fn the_doctor_reports_each_chat_opt_in_by_its_own_key() {
        let off = config_with_chat(true);
        assert_eq!(
            chat_option_lines(&off),
            (
                "off ([chat] project_rooms = false)",
                "off ([chat] hook_drain = false)"
            )
        );
        let mut rooms = config_with_chat(true);
        rooms.chat.project_rooms = true;
        let (project_rooms, hook_drain) = chat_option_lines(&rooms);
        assert!(project_rooms.starts_with("on"), "{project_rooms}");
        assert!(hook_drain.starts_with("off"), "{hook_drain}");
        let mut drain = config_with_chat(true);
        drain.chat.hook_drain = true;
        let (project_rooms, hook_drain) = chat_option_lines(&drain);
        assert!(project_rooms.starts_with("off"), "{project_rooms}");
        assert!(hook_drain.starts_with("on"), "{hook_drain}");
    }

    #[test]
    fn a_present_kill_file_never_reports_chat_enabled() {
        // The regression this guards against: a doctor line built from a
        // second copy of the enabled/killed table (or from `config.chat.enabled`
        // alone) can drift from `chat_enabled` and report "enabled" while the
        // kill file is standing everything down.
        let on = config_with_chat(true);
        assert_ne!(chat_status_line(&on, true), "enabled");
        assert!(!on.chat_enabled(true));
    }
}
