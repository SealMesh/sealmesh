use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;

use crate::{paths::config_home, wire::RepoSlug};

const PROJECT_CONFIG: &str = "sealmesh.local.toml";
const USER_CONFIG: &str = "sealmesh/config.toml";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_integration_branch")]
    pub integration_branch: String,
    pub ticket_prefixes: Vec<String>,
    pub server: ServerConfig,
    pub allow: AllowConfig,
    #[serde(default)]
    pub claude: ClaudeConfig,
    /// Tools described here rather than in Rust. Empty by default, so this
    /// ships dark until someone writes a block.
    #[serde(default)]
    pub provider: Vec<crate::collect::generic::ProviderSpec>,
    #[serde(default)]
    pub events: EventsConfig,
    #[serde(default)]
    pub declared: DeclaredConfig,
    #[serde(default)]
    pub chat: ChatConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredConfig {
    #[serde(default = "default_declared_ttl_secs")]
    pub ttl_secs: u64,
}

const MIN_DECLARED_TTL_SECS: u64 = 60;
const MAX_DECLARED_TTL_SECS: u64 = 86_400;

impl DeclaredConfig {
    pub fn ttl(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.ttl_secs)
    }

    fn validate(&self) -> Result<()> {
        if !(MIN_DECLARED_TTL_SECS..=MAX_DECLARED_TTL_SECS).contains(&self.ttl_secs) {
            bail!(
                "declared.ttl_secs is {}, outside {MIN_DECLARED_TTL_SECS}..={MAX_DECLARED_TTL_SECS}",
                self.ttl_secs
            );
        }
        Ok(())
    }
}

impl Default for DeclaredConfig {
    fn default() -> Self {
        Self {
            ttl_secs: default_declared_ttl_secs(),
        }
    }
}

/// The chat surface's own switch, off by default. The daemon's room
/// client and local socket gate every request and every room frame on
/// `chat_enabled` below, and `doctor` reports it.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Whether resolving a checkout's rooms also names its project's room.
    /// Ticket and repo rooms follow from the branch and the origin; a project
    /// room spans every repo in the project, so it is opt-in (Q3). Joining
    /// one by name is unaffected, and the router decides every join either
    /// way.
    #[serde(default)]
    pub project_rooms: bool,
    /// Whether the Claude Code hook drains this session's pending room
    /// messages into its context each turn. Off by default, like the
    /// subagent hooks.
    #[serde(default)]
    pub hook_drain: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsConfig {
    #[serde(default = "default_event_interval_secs")]
    pub interval_secs: u64,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

const MIN_EVENT_INTERVAL_SECS: u64 = 10;
const MAX_EVENT_INTERVAL_SECS: u64 = 3600;

impl EventsConfig {
    pub fn interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.interval_secs)
    }

    fn validate(&self) -> Result<()> {
        if !(MIN_EVENT_INTERVAL_SECS..=MAX_EVENT_INTERVAL_SECS).contains(&self.interval_secs) {
            bail!(
                "events.interval_secs is {}, outside {MIN_EVENT_INTERVAL_SECS}..={MAX_EVENT_INTERVAL_SECS}",
                self.interval_secs
            );
        }
        Ok(())
    }
}

impl Default for EventsConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_event_interval_secs(),
            enabled: true,
        }
    }
}

/// Settings that belong to the Claude Code provider and to nothing else.
///
/// Scoped rather than top-level because `extra_roots` never meant "watch this
/// directory" — it meant "here is another directory laid out the way Claude
/// Code lays one out", and a flat key said otherwise to anyone reading the
/// file. A second vendor gets its own table, not a share of this one.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaudeConfig {
    #[serde(default)]
    pub extra_roots: Vec<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub url: String,
}

impl ServerConfig {
    pub fn connect_url(&self) -> Result<String> {
        let base = self.url.trim_end_matches('/');
        let socket = match base.split_once("://") {
            Some(("https", rest)) => format!("wss://{rest}"),
            Some(("http", rest)) if is_loopback(base) => format!("ws://{rest}"),
            Some(("http", _)) => bail!(
                "server.url is {base}, which is http:// to a host that is not loopback. \
                 The handshake sends a GitHub token, so that hands a working credential for \
                 the organisation to anyone on the network path. Use https://, or \
                 http://127.0.0.1:<port> for a router running on this machine."
            ),
            _ => bail!("server.url must start with https:// or http://"),
        };
        Ok(format!("{socket}/v1/connect"))
    }
}

fn is_loopback(url: &str) -> bool {
    let Ok(parsed) = url::Url::parse(url) else {
        return false;
    };
    match parsed.host() {
        Some(url::Host::Domain(name)) => name.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowConfig {
    pub repos: Vec<RepoSlug>,
    pub paths: Vec<PathBuf>,
}

fn default_integration_branch() -> String {
    "dev".to_owned()
}

fn default_event_interval_secs() -> u64 {
    120
}

fn default_true() -> bool {
    true
}

fn default_declared_ttl_secs() -> u64 {
    900
}

impl Config {
    pub fn load(explicit: Option<&Path>) -> Result<Self> {
        let path = match explicit {
            Some(p) => p.to_path_buf(),
            None => Self::discover()?,
        };
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading config at {}", path.display()))?;
        let config: Self = toml::from_str(&text)
            .with_context(|| format!("parsing config at {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    fn discover() -> Result<PathBuf> {
        let user = config_home()
            .ok_or_else(|| anyhow!("no home directory to resolve the config path against"))?
            .join(USER_CONFIG);
        if user.is_file() {
            return Ok(user);
        }
        bail!(
            "no config found at {}. A project-local {PROJECT_CONFIG} is never \
             picked up from the working directory — pass it explicitly with \
             `--config {PROJECT_CONFIG}` if that is what you meant.",
            user.display()
        )
    }

    fn validate(&self) -> Result<()> {
        if self.allow.repos.is_empty() {
            bail!("allow.repos is empty, so nothing would ever be published");
        }
        if self.allow.paths.is_empty() {
            bail!("allow.paths is empty, so nothing would ever be published");
        }
        self.server.connect_url()?;
        crate::collect::generic::validate(&self.provider)?;
        self.events.validate()?;
        self.declared.validate()?;
        Ok(())
    }

    pub fn collect_options(&self) -> crate::collect::Options {
        crate::collect::Options {
            integration_branch: self.integration_branch.clone(),
            ticket_prefixes: self.ticket_prefixes.clone(),
            declared_dir: crate::paths::declared_agents_dir(),
            declared_ttl: jiff::SignedDuration::try_from(self.declared.ttl())
                .unwrap_or(jiff::SignedDuration::from_secs(900)),
        }
    }

    pub fn off_switch_path() -> Option<PathBuf> {
        config_home().map(|d| d.join("sealmesh/off"))
    }

    /// Whether chat is enabled, given whether the one kill file
    /// (`off_switch_path`) exists. One kill file kills everything, chat
    /// included: the kill file always wins over `[chat] enabled`,
    /// while turning `[chat]` off on its own leaves `who` untouched and vice
    /// versa — the two are independent switches that only meet here.
    pub fn chat_enabled(&self, killed: bool) -> bool {
        self.chat.enabled && !killed
    }

    pub fn gate(&self) -> Result<Gate> {
        let mut paths = Vec::with_capacity(self.allow.paths.len());
        for raw in &self.allow.paths {
            let expanded = expand_tilde(raw);
            let canonical = expanded.canonicalize().with_context(|| {
                format!(
                    "allow.paths entry {} does not resolve to a real directory",
                    raw.display()
                )
            })?;
            paths.push(canonical);
        }
        Ok(Gate {
            paths,
            repos: self.allow.repos.iter().cloned().collect(),
        })
    }
}

fn expand_tilde(path: &Path) -> PathBuf {
    let Ok(rest) = path.strip_prefix("~") else {
        return path.to_path_buf();
    };
    match dirs::home_dir() {
        Some(home) => home.join(rest),
        None => path.to_path_buf(),
    }
}

#[derive(Debug, Clone)]
pub struct Gate {
    paths: Vec<PathBuf>,
    repos: BTreeSet<RepoSlug>,
}

impl Gate {
    #[cfg(test)]
    pub fn new(paths: Vec<PathBuf>, repos: BTreeSet<RepoSlug>) -> Self {
        Self { paths, repos }
    }

    pub fn allowed_dir(&self, reported: &Path) -> Option<PathBuf> {
        let canonical = reported.canonicalize().ok()?;
        self.paths
            .iter()
            .any(|root| canonical.starts_with(root))
            .then_some(canonical)
    }

    pub fn allows_repo(&self, slug: &RepoSlug) -> bool {
        self.repos.contains(slug)
    }

    pub fn repos(&self) -> impl Iterator<Item = &RepoSlug> {
        self.repos.iter()
    }

    pub fn paths(&self) -> &[PathBuf] {
        &self.paths
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn gate_over(root: &Path) -> Gate {
        Gate::new(
            vec![root.canonicalize().unwrap()],
            [RepoSlug::try_from("example-org/example-repo").unwrap()]
                .into_iter()
                .collect(),
        )
    }

    #[test]
    fn a_traversal_out_of_an_allowed_root_is_refused() {
        let tmp = std::env::temp_dir().join("sealmesh-gate-traversal");
        let shared = tmp.join("shared");
        let clients = tmp.join("clients/acme-litigation");
        std::fs::create_dir_all(&shared).unwrap();
        std::fs::create_dir_all(&clients).unwrap();

        let gate = gate_over(&shared);
        assert!(gate.allowed_dir(&shared).is_some());
        assert!(
            gate.allowed_dir(&shared.join("../clients/acme-litigation"))
                .is_none()
        );
        assert!(gate.allowed_dir(&clients).is_none());

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn a_sibling_with_a_shared_prefix_is_not_inside_the_root() {
        let tmp = std::env::temp_dir().join("sealmesh-gate-prefix");
        let allowed = tmp.join("proj");
        let sibling = tmp.join("proj-secret");
        std::fs::create_dir_all(&allowed).unwrap();
        std::fs::create_dir_all(&sibling).unwrap();

        let gate = gate_over(&allowed);
        assert!(gate.allowed_dir(&sibling).is_none(), "prefix match leaked");

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn a_symlink_into_a_denied_tree_is_resolved_before_matching() {
        let tmp = std::env::temp_dir().join("sealmesh-gate-symlink");
        let allowed = tmp.join("shared");
        let secret = tmp.join("clients/acme");
        std::fs::create_dir_all(&allowed).unwrap();
        std::fs::create_dir_all(&secret).unwrap();
        let link = allowed.join("shortcut");
        std::fs::remove_file(&link).ok();
        std::os::unix::fs::symlink(&secret, &link).unwrap();

        let gate = gate_over(&allowed);
        assert!(
            gate.allowed_dir(&link).is_none(),
            "symlink escaped the root"
        );

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn repo_gate_is_an_exact_slug_match() {
        let gate = Gate::new(
            vec![],
            [RepoSlug::try_from("example-org/example-repo").unwrap()]
                .into_iter()
                .collect(),
        );
        assert!(gate.allows_repo(&RepoSlug::try_from("example-org/example-repo").unwrap()));
        assert!(!gate.allows_repo(&RepoSlug::try_from("example-org/example-repo-fork").unwrap()));
        assert!(!gate.allows_repo(&RepoSlug::try_from("someone-else/example-repo").unwrap()));
    }

    #[test]
    fn the_off_switch_lives_where_the_docs_say() {
        let path = Config::off_switch_path().unwrap();
        assert!(
            path.ends_with(".config/sealmesh/off") || std::env::var_os("XDG_CONFIG_HOME").is_some(),
            "off switch resolved to {}",
            path.display()
        );
    }

    #[test]
    fn an_empty_allowlist_is_a_config_error_not_a_quiet_no_op() {
        let toml = r#"
            ticket_prefixes = ["ENG"]
            [server]
            url = "https://router.example.com"
            [allow]
            repos = []
            paths = ["/tmp"]
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn a_config_cannot_declare_its_own_host() {
        let toml = r#"
            host = "sandbank"
            ticket_prefixes = ["ENG"]
            [server]
            url = "https://router.example.com"
            [allow]
            repos = ["example-org/example-repo"]
            paths = ["/tmp"]
        "#;
        assert!(toml::from_str::<Config>(toml).is_err());
    }

    #[test]
    fn the_socket_scheme_follows_the_base_url_rather_than_being_configured() {
        let url = |u: &str| ServerConfig { url: u.to_owned() }.connect_url();

        assert_eq!(
            url("https://sealmesh.internal").unwrap(),
            "wss://sealmesh.internal/v1/connect"
        );
        assert_eq!(
            url("http://127.0.0.1:8787/").unwrap(),
            "ws://127.0.0.1:8787/v1/connect"
        );
        for bad in ["sealmesh.internal", "ftp://x", "wss://already", ""] {
            assert!(url(bad).is_err(), "accepted {bad:?}");
        }
    }

    #[test]
    fn plaintext_to_anywhere_but_this_machine_is_refused() {
        let url = |u: &str| ServerConfig { url: u.to_owned() }.connect_url();

        for leaky in [
            "http://sealmesh.internal",
            "http://router.example.com",
            "http://10.0.1.7:8787",
            "http://dev-sealmesh-alb-1234.us-east-1.elb.amazonaws.com",
            "http://127.0.0.1.evil.test:8787",
            "http://localhost.evil.test:8787",
            "http://evil.test/localhost",
        ] {
            let refused = url(leaky);
            assert!(refused.is_err(), "accepted plaintext to {leaky}");
            let message = format!("{:#}", refused.unwrap_err());
            assert!(
                message.contains("GitHub token"),
                "the refusal must say what leaks: {message}"
            );
        }
    }

    #[test]
    fn plaintext_to_this_machine_is_still_how_local_development_works() {
        let url = |u: &str| ServerConfig { url: u.to_owned() }.connect_url();

        assert_eq!(
            url("http://localhost:8787").unwrap(),
            "ws://localhost:8787/v1/connect"
        );
        assert_eq!(
            url("http://[::1]:8787").unwrap(),
            "ws://[::1]:8787/v1/connect"
        );
        assert_eq!(
            url("http://127.0.0.2:8787").unwrap(),
            "ws://127.0.0.2:8787/v1/connect",
            "the whole 127/8 block is loopback, not just .1"
        );
    }

    #[test]
    fn https_is_never_caught_by_the_plaintext_refusal() {
        let url = |u: &str| ServerConfig { url: u.to_owned() }.connect_url();
        for fine in [
            "https://router.example.com",
            "https://127.0.0.1:8787",
            "https://localhost:8787",
        ] {
            assert!(url(fine).is_ok(), "refused {fine}");
        }
    }

    #[test]
    fn claude_roots_are_scoped_to_claude_and_the_flat_key_is_refused() {
        let scoped = config_with("[claude]\nextra_roots = [\"/tmp/other\"]")
            .expect("the scoped key did not parse");
        assert_eq!(
            scoped.claude.extra_roots,
            vec![PathBuf::from("/tmp/other")],
            "the scoped key parsed but landed nowhere"
        );

        // `deny_unknown_fields` means the old flat key is a refusal to start
        // rather than a setting that quietly stopped applying. That is the
        // loud failure, and it is the one worth having: a daemon that starts
        // and silently watches nothing looks exactly like a daemon with
        // nothing to watch.
        assert!(
            config_with("extra_roots = [\"/tmp/other\"]").is_err(),
            "the pre-rename key was accepted, so a stale config would run with \
             its roots silently ignored"
        );
    }

    #[test]
    fn a_provider_block_parses_and_a_typo_in_one_refuses_to_start() {
        let block = r#"
            [[provider]]
            name = "qwen"
            root = "/tmp/qwen-runs"
            layout = "file_per_agent"

            [provider.fields]
            work_dir = "dir"
            run_state = "status"
            started_at = "started"

            [provider.run_state]
            working = ["running"]
        "#;
        let config = config_with(block).expect("a valid provider block was refused");
        assert_eq!(config.provider.len(), 1);
        assert_eq!(
            config.provider.first().map(|p| p.name.as_str()),
            Some("qwen")
        );

        // Unknown keys are refused throughout, so a misspelled mapping is a
        // startup error rather than a field that silently never populates.
        let typo = block.replace("work_dir =", "workdir =");
        assert!(
            config_with(&typo).is_err(),
            "a misspelled field name was accepted"
        );

        // And the reserved-name check runs at load, not at scan time.
        let reserved = block.replace(r#"name = "qwen""#, r#"name = "declared""#);
        assert!(
            config_with(&reserved).is_err(),
            "a config block claimed a reserved tool name"
        );
    }

    fn config_with(extra: &str) -> Result<Config> {
        let toml = format!(
            r#"
            ticket_prefixes = ["ENG"]
            [server]
            url = "https://router.example.com"
            [allow]
            repos = ["example-org/example-repo"]
            paths = ["/tmp"]
            {extra}
        "#
        );
        let config: Config = toml::from_str(&toml)?;
        config.validate()?;
        Ok(config)
    }

    #[test]
    fn a_config_without_an_events_section_gets_the_defaults() {
        let config = config_with("").unwrap();
        assert!(config.events.enabled);
        assert_eq!(config.events.interval_secs, 120);
    }

    #[test]
    fn an_interval_that_would_panic_or_burn_the_machine_is_refused_at_load() {
        for bad in [0, 1, 9, MAX_EVENT_INTERVAL_SECS + 1] {
            assert!(
                config_with(&format!("[events]\ninterval_secs = {bad}")).is_err(),
                "accepted interval_secs = {bad}"
            );
        }
        for fine in [MIN_EVENT_INTERVAL_SECS, 120, MAX_EVENT_INTERVAL_SECS] {
            let config = config_with(&format!("[events]\ninterval_secs = {fine}")).unwrap();
            assert_eq!(config.events.interval().as_secs(), fine);
        }
    }

    #[test]
    fn events_can_be_turned_off_without_restating_the_interval() {
        let config = config_with("[events]\nenabled = false").unwrap();
        assert!(!config.events.enabled);
        assert_eq!(config.events.interval_secs, 120);
    }

    #[test]
    fn chat_defaults_to_disabled() {
        let config = config_with("").unwrap();
        assert!(!config.chat.enabled);
        assert!(!config.chat_enabled(false));
    }

    #[test]
    fn chat_enabled_covers_all_four_flag_and_kill_combinations() {
        let off = config_with("").unwrap();
        let on = config_with("[chat]\nenabled = true").unwrap();

        assert!(!off.chat_enabled(false), "off, not killed");
        assert!(!off.chat_enabled(true), "off, killed");
        assert!(on.chat_enabled(false), "on, not killed");
        assert!(!on.chat_enabled(true), "on, killed");
    }

    #[test]
    fn the_chat_opt_ins_default_off_and_read_independently() {
        let config = config_with("[chat]\nenabled = true").unwrap();
        assert!(!config.chat.project_rooms);
        assert!(!config.chat.hook_drain);
        let config = config_with("[chat]\nproject_rooms = true").unwrap();
        assert!(config.chat.project_rooms);
        assert!(!config.chat.hook_drain);
        assert!(!config.chat.enabled, "an opt-in does not turn chat on");
        let config = config_with("[chat]\nhook_drain = true").unwrap();
        assert!(config.chat.hook_drain);
        assert!(!config.chat.project_rooms);
        assert!(config_with("[chat]\nproject_rooms = \"yes\"").is_err());
    }

    #[test]
    fn an_unknown_chat_key_is_refused() {
        assert!(config_with("[chat]\nenabled = true\nroom = \"general\"").is_err());
    }

    #[test]
    fn the_kill_file_stands_chat_down_even_when_chat_is_enabled() {
        let config = config_with("[chat]\nenabled = true").unwrap();
        assert!(
            config.chat_enabled(false),
            "chat should be on when not killed"
        );
        assert!(
            !config.chat_enabled(true),
            "the kill file must stand chat down regardless of [chat] enabled"
        );
    }

    #[test]
    fn events_off_and_chat_on_are_independent_config_keys() {
        let config = config_with("[events]\nenabled = false\n[chat]\nenabled = true").unwrap();
        assert!(!config.events.enabled, "events.enabled leaked from [chat]");
        assert!(config.chat.enabled, "chat.enabled leaked from [events]");
    }

    #[test]
    fn events_on_and_chat_off_are_independent_config_keys() {
        let config = config_with("[events]\nenabled = true\n[chat]\nenabled = false").unwrap();
        assert!(config.events.enabled, "events.enabled leaked from [chat]");
        assert!(!config.chat.enabled, "chat.enabled leaked from [events]");
    }

    #[test]
    fn discovery_never_resolves_relative_to_the_working_directory() {
        if let Ok(path) = Config::discover() {
            assert!(
                path.is_absolute(),
                "discover() returned {}, which resolves against the cwd",
                path.display()
            );
        }
    }
}
