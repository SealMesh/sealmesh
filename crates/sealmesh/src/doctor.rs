use std::collections::BTreeMap;

use anyhow::Result;

use crate::{
    collect::{self, Decision, Outcome, git::GitResolver, roots},
    config::Config,
    config::Mode,
    credential::CredentialSource,
    paths, render,
};

pub async fn run(config: &Config) -> Result<()> {
    println!("mode              {}", mode_line(config));
    println!("router            {}", router_line(config)?);
    if config.mode() == Mode::Otw {
        let socket = sealmesh_proto::tailnet::default_tailscaled_socket();
        println!(
            "tailscaled        {}",
            tailscaled_line(&socket, localapi_status(&socket))
        );
    }
    for warning in config.warnings() {
        println!("warning           {warning}");
    }
    println!("integration       origin/{}", config.integration_branch);
    println!("ticket rooms      {}", ticket_rooms_line(config));
    println!("host              {}", host_line(config.mode()));
    println!("machine           {}", machine_line(config));

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
    println!(
        "guardrail         {}",
        crate::policy::status_line(&crate::policy::load(
            std::path::Path::new(crate::policy::POLICY_PATH),
            crate::policy::TRUSTED_OWNER,
        ))
    );
    if let Some(path) = crate::audit::path() {
        let count = crate::audit::read(&path).map(|e| e.len()).unwrap_or(0);
        println!("audit log         {} ({count} answered)", path.display());
    }
    if let Some(dir) = paths::cache_home() {
        println!("cache             {}", dir.join("sealmesh").display());
    }
    if config.mode() == Mode::Std {
        for line in std_lines(config) {
            println!("{line}");
        }
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

    let codex_home = collect::codex::discover_home();
    match &codex_home {
        Some(home) if home.join("sessions").is_dir() => {
            println!("\ncodex home\n  {}", home.display());
        }
        Some(home) => println!(
            "\ncodex home\n  {} (no sessions/ yet; set CODEX_HOME if Codex keeps its state elsewhere)",
            home.display()
        ),
        None => println!("\ncodex home\n  none (no home directory and no CODEX_HOME)"),
    }
    for spec in config
        .provider
        .iter()
        .filter(|spec| collect::providers::shadowed_by_native(spec))
    {
        println!(
            "  [[provider]] {:?} is skipped: sealmesh reads Codex natively",
            spec.name
        );
    }

    let procs = collect::liveness::ProcessTable::read();
    let options = config.collect_options();
    let providers = collect::providers::with_configured(
        roots.clone(),
        codex_home,
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

    let source = config.credential_source()?;
    println!("\n{}", credential_heading(&source));
    match source.token() {
        Ok(_) => println!("  resolved. Run `sealmesh daemon` to connect."),
        Err(error) => println!("  NOT resolved: {error:#}"),
    }

    Ok(())
}

/// The `doctor` mode line: the mode, and whether it was chosen or defaulted.
fn mode_line(config: &Config) -> String {
    let mode = config.mode();
    let what = match mode {
        Mode::Std => "router inside this daemon, identity = your OS user",
        Mode::Otw => "router on your tailnet, identity = Tailscale",
        Mode::Cloud => "remote router, identity = your GitHub login",
    };
    format!("{mode} ({what})")
}

fn router_line(config: &Config) -> Result<String> {
    if config.mode().has_remote_router() {
        config.router_url()
    } else {
        Ok("embedded (std mode runs the router inside `sealmesh daemon`)".to_owned())
    }
}

/// How long `doctor` waits on the local tailscaled.
const TAILSCALED_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Asks the local tailscaled's LocalAPI for its status, and says whether it
/// answered 200. Only `doctor` calls it: the daemon itself never needs the
/// local tailscaled, only to be on the tailnet.
#[cfg(unix)]
fn localapi_status(socket: &std::path::Path) -> Result<(), String> {
    use std::io::{Read as _, Write as _};

    let mut stream =
        std::os::unix::net::UnixStream::connect(socket).map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(TAILSCALED_TIMEOUT))
        .and_then(|()| stream.set_write_timeout(Some(TAILSCALED_TIMEOUT)))
        .map_err(|error| error.to_string())?;
    stream
        .write_all(
            b"GET /localapi/v0/status?peers=false HTTP/1.0\r\n\
              Host: local-tailscaled.sock\r\n\r\n",
        )
        .map_err(|error| error.to_string())?;
    let mut head = [0_u8; 64];
    let read = stream.read(&mut head).map_err(|error| error.to_string())?;
    let line = String::from_utf8_lossy(head.get(..read).unwrap_or_default()).into_owned();
    let status = line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_owned();
    if status == "200" {
        Ok(())
    } else {
        Err(format!(
            "answered {:?}",
            line.lines().next().unwrap_or_default()
        ))
    }
}

#[cfg(not(unix))]
fn localapi_status(_socket: &std::path::Path) -> Result<(), String> {
    Err("not checked on this platform".to_owned())
}

/// The `doctor` tailscaled line, from [`localapi_status`]'s answer.
fn tailscaled_line(socket: &std::path::Path, status: Result<(), String>) -> String {
    match status {
        Ok(()) => format!("answers at {}", socket.display()),
        Err(error) => format!(
            "no answer at {} ({error}). The otw router's machine needs tailscaled's LocalAPI \
             to identify peers; this machine only needs to be on the tailnet. The macOS app \
             serves LocalAPI elsewhere, so check `tailscale status` there",
            socket.display()
        ),
    }
}

/// Whether something accepts connections on the Unix socket at `path`.
fn socket_line(path: Option<&std::path::Path>) -> String {
    let Some(path) = path else {
        return "unavailable: no home directory".to_owned();
    };
    let state = if std::os::unix::net::UnixStream::connect(path).is_ok() {
        "ok, listening"
    } else if path.exists() {
        "NOT LISTENING (a socket file is there; is `sealmesh daemon` running?)"
    } else {
        "not running (start `sealmesh daemon`)"
    };
    format!("{} ({state})", path.display())
}

/// Std mode's own lines: the router socket, `agents.sock`, Claude's rooms,
/// and each agent with its rooms and its token file's mode. Never a token.
fn std_lines(config: &Config) -> Vec<String> {
    let mut lines = vec![format!(
        "router socket     {}",
        socket_line(paths::local_router_socket().as_deref())
    )];
    if config.agents.is_empty() {
        lines.push("agents.sock       off (no [[agent]] blocks)".to_owned());
    } else {
        lines.push(format!(
            "agents.sock       {}",
            socket_line(paths::agents_socket().as_deref())
        ));
    }
    let rooms_of = |name: &sealmesh_wire::HostId| {
        let rooms = config.agent_rooms(name);
        if rooms.is_empty() {
            "no rooms".to_owned()
        } else {
            rooms
                .iter()
                .map(|room| format!("#{room}"))
                .collect::<Vec<_>>()
                .join(", ")
        }
    };
    lines.push("\nagents (std mode)".to_owned());
    if let Ok(claude) = sealmesh_wire::HostId::try_from(crate::config::BUILT_IN_AGENT) {
        lines.push(format!(
            "  {claude:<12}  built in (MCP tools)  {}",
            rooms_of(&claude)
        ));
    }
    let tokens = paths::agent_tokens_dir();
    for agent in &config.agents {
        let token = tokens.as_ref().map_or_else(
            || "token: no home directory".to_owned(),
            |dir| {
                let path = crate::agent_link::tokens::token_path(dir, &agent.name);
                format!(
                    "token {} {}",
                    path.display(),
                    crate::agent_link::tokens::describe_file(&path)
                )
            },
        );
        lines.push(format!(
            "  {:<12}  {}\n                {token}",
            agent.name,
            rooms_of(&agent.name)
        ));
    }
    lines
}

fn host_line(mode: Mode) -> &'static str {
    match mode {
        Mode::Std => "derived from your OS user name",
        Mode::Otw => "derived from your Tailscale identity at connect",
        Mode::Cloud => "derived from your GitHub login at connect",
    }
}

fn credential_heading(source: &CredentialSource) -> &'static str {
    match source {
        CredentialSource::GhCli => "github credential",
        CredentialSource::LocalSecret(_) => "local router secret",
        CredentialSource::None => "credential (none: the tailnet identifies this machine)",
    }
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

/// The ticket-room line. Ticket rooms are opt-in through `ticket_prefixes`:
/// with none, no branch names a ticket and a checkout has only its repo room.
fn ticket_rooms_line(config: &Config) -> String {
    if config.ticket_prefixes.is_empty() {
        "off (no ticket_prefixes)".to_owned()
    } else {
        format!("on (prefixes {})", config.ticket_prefixes.join(", "))
    }
}

/// Which machine name this daemon gives the router, and where it came from.
fn machine_line(config: &Config) -> String {
    match (&config.machine, config.machine_name()) {
        (Some(name), _) => format!("{name} (machine in the config)"),
        (None, Some(name)) => format!("{name} (from this machine's hostname)"),
        (None, None) => "none — the hostname makes no machine name, so the router cannot tell \
                         this machine from your others. Set `machine` in the config"
            .to_owned(),
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
    fn the_doctor_prints_the_mode_and_a_router_line_that_fits_it() {
        let cloud = config_with_chat(false);
        assert!(
            mode_line(&cloud).starts_with("cloud ("),
            "{}",
            mode_line(&cloud)
        );
        assert_eq!(
            router_line(&cloud).unwrap(),
            "wss://router.example.com/v1/connect"
        );
        assert_eq!(
            credential_heading(&cloud.credential_source().unwrap()),
            "github credential"
        );

        let std: Config = toml::from_str("[mesh]\nmode = \"std\"").unwrap();
        assert!(mode_line(&std).starts_with("std ("), "{}", mode_line(&std));
        assert!(router_line(&std).unwrap().starts_with("embedded"));
        assert_eq!(host_line(Mode::Std), "derived from your OS user name");
        assert_eq!(
            credential_heading(&std.credential_source().unwrap()),
            "local router secret"
        );
    }

    #[test]
    fn the_doctor_lists_each_std_agent_with_its_rooms_and_token_file_and_never_a_token() {
        let config: Config = toml::from_str(
            "[mesh]\nmode = \"std\"\n\
             [[room]]\nname = \"ops\"\nmembers = [\"claude\", \"grokbot\"]\n\
             [[room]]\nname = \"bots\"\nmembers = [\"grokbot\"]\n\
             [[agent]]\nname = \"grokbot\"\nrooms = [\"ops\"]",
        )
        .unwrap();
        let text = std_lines(&config).join("\n");
        assert!(text.contains("router socket"), "{text}");
        assert!(text.contains("agents.sock"), "{text}");
        assert!(text.contains("claude"), "{text}");
        assert!(text.contains("grokbot.token"), "{text}");
        // The allowlist narrows grokbot to #ops, though #bots lists it.
        let grok = text
            .lines()
            .find(|line| line.trim_start().starts_with("grokbot"))
            .unwrap();
        assert!(grok.contains("#ops") && !grok.contains("#bots"), "{grok}");

        let none: Config = toml::from_str("[mesh]\nmode = \"std\"").unwrap();
        assert!(
            std_lines(&none)
                .join("\n")
                .contains("off (no [[agent]] blocks)")
        );
    }

    #[test]
    fn the_doctor_machine_line_says_where_the_name_came_from() {
        let mut config = config_with_chat(false);
        config.machine = Some(sealmesh_proto::MachineName::try_from("work-laptop").unwrap());
        assert_eq!(machine_line(&config), "work-laptop (machine in the config)");
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
    fn the_doctor_says_ticket_rooms_are_off_without_prefixes() {
        let mut config = config_with_chat(false);
        assert_eq!(ticket_rooms_line(&config), "on (prefixes ENG)");
        config.ticket_prefixes.clear();
        assert_eq!(ticket_rooms_line(&config), "off (no ticket_prefixes)");
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

    #[cfg(unix)]
    #[test]
    fn the_tailscaled_line_says_whether_localapi_answered() {
        use std::io::{Read as _, Write as _};

        let dir = std::env::temp_dir().join(format!("smdoc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let socket = dir.join("ts.sock");
        let _ = std::fs::remove_file(&socket);
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let server = std::thread::spawn(move || {
            for answer in [
                "HTTP/1.0 200 OK\r\n\r\n{}",
                "HTTP/1.0 403 Forbidden\r\n\r\n",
            ] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0_u8; 256];
                let read = stream.read(&mut request).unwrap();
                let request = String::from_utf8_lossy(request.get(..read).unwrap()).into_owned();
                assert!(request.starts_with("GET /localapi/v0/status"), "{request}");
                assert!(request.contains("Host: local-tailscaled.sock"), "{request}");
                stream.write_all(answer.as_bytes()).unwrap();
            }
        });

        assert_eq!(localapi_status(&socket), Ok(()));
        assert!(tailscaled_line(&socket, localapi_status(&socket)).contains("no answer"));
        server.join().unwrap();
        std::fs::remove_dir_all(&dir).ok();

        let missing = dir.join("absent.sock");
        let line = tailscaled_line(&missing, localapi_status(&missing));
        assert!(line.starts_with("no answer at"), "{line}");
        assert!(
            tailscaled_line(&missing, Ok(())).starts_with("answers at"),
            "{line}"
        );
    }
}
