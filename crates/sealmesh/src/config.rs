use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;

use sealmesh_proto::tailnet::{is_magic_dns_name, is_tailnet_ip};

use crate::{paths::config_home, wire::RepoSlug};

const PROJECT_CONFIG: &str = "sealmesh.local.toml";
const USER_CONFIG: &str = "sealmesh/config.toml";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_integration_branch")]
    pub integration_branch: String,
    /// Ticket ID prefixes (`ENG` for `ENG-557`) to read out of branch names.
    /// Optional: empty means no branch ever names a ticket, so ticket rooms
    /// are off and checkouts resolve to their repo room alone.
    #[serde(default)]
    pub ticket_prefixes: Vec<String>,
    /// Which of your machines this one is, as `sealmesh who` shows it beside
    /// your login. Optional: the OS hostname is used without it. Two machines
    /// signed in as the same person must not share one, or each reconnect of
    /// one takes the other's place at the router.
    #[serde(default)]
    pub machine: Option<sealmesh_proto::MachineName>,
    /// Which of the three meshes this machine is part of. Optional: a config
    /// without `[mesh]` is `cloud`, today's behaviour.
    #[serde(default)]
    pub mesh: MeshConfig,
    /// The router to reach. Required in `cloud` and `otw` mode; ignored in
    /// `std` mode, where the router runs inside the daemon. Read it through
    /// [`Config::server`], which says why it is missing.
    #[serde(default)]
    pub server: Option<ServerConfig>,
    /// What may be published. Required and non-empty in `cloud` and `otw`
    /// mode; may be empty (or absent) in `std` mode, where the only reader is
    /// this machine's own user.
    #[serde(default)]
    pub allow: AllowConfig,
    /// Named rooms (`[[room]] name = "ops" members = ["claude", "grokbot"]`).
    /// `std` and `otw` only: a cloud router decides rooms from GitHub, and a
    /// cloud config that lists one is refused at load.
    #[serde(default, rename = "room")]
    pub rooms: Vec<RoomConfig>,
    /// Agents that reach named rooms over `agents.sock`
    /// (`[[agent]] name = "grokbot"`). `std` only. Each gets a bearer token
    /// file the daemon writes, and every post it makes is stamped with this
    /// name: an agent never says who it is.
    #[serde(default, rename = "agent")]
    pub agents: Vec<AgentConfig>,
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

/// Where the router runs and who vouches for identity.
///
/// - `std`: the router runs inside this daemon; identity is the local OS
///   user, proved by a per-boot secret file only that user can read.
/// - `otw`: the router runs on one of your machines and is reached over
///   Tailscale; identity comes from the tailnet, not from a token.
/// - `cloud`: a remote router; identity is your GitHub login, and access
///   follows your GitHub repo permissions. The default, and what every
///   config written before modes existed means.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Std,
    Otw,
    #[default]
    Cloud,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Std => "std",
            Self::Otw => "otw",
            Self::Cloud => "cloud",
        }
    }

    /// Whether this mode reaches a router somewhere else, so `[server] url`
    /// is required.
    pub fn has_remote_router(self) -> bool {
        !matches!(self, Self::Std)
    }
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeshConfig {
    #[serde(default)]
    pub mode: Mode,
}

/// One `[[room]]` block: a named room and the members who may join it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoomConfig {
    pub name: sealmesh_chat_wire::RoomName,
    /// Who may join. Compared, at the router, against the connecting
    /// identity's label (see `sealmesh_server::named_rooms`).
    pub members: Vec<sealmesh_wire::HostId>,
}

/// The built-in agent the std daemon's own router connection speaks for:
/// Claude, through the MCP room tools and the daemon socket. A `[[room]]`
/// lists it by this name; an `[[agent]]` block may not take it.
pub const BUILT_IN_AGENT: &str = "claude";

/// One `[[agent]]` block: an agent that reaches named rooms over
/// `agents.sock`, identified by a token the daemon issues, never by anything
/// it says about itself.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    /// The agent's name: the label `[[room]] members` lists and the author
    /// stamped on every post it makes. 1-32 chars of `[a-z0-9-]`.
    pub name: sealmesh_wire::HostId,
    /// An optional allowlist narrowing which of the rooms that list this agent
    /// it may use. Absent means every room that lists it.
    #[serde(default)]
    pub rooms: Option<Vec<sealmesh_chat_wire::RoomName>>,
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

/// Deprecated. Agent-state events are emitted on a timer, but nothing on
/// the router consumes them: it counts each batch and drops it. Off by
/// default; an explicit `enabled = true` still emits exactly as before (the
/// wire frames are unchanged so old routers and daemons keep working), and
/// the daemon warns once at start that the section is going away.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsConfig {
    #[serde(default = "default_event_interval_secs")]
    pub interval_secs: u64,
    #[serde(default)]
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
            enabled: false,
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

    /// [`Self::connect_url`] for otw mode, where the router is on the
    /// tailnet: `http://` is fine there (WireGuard encrypts the hop, and the
    /// handshake carries no credential), but the host must be a tailnet
    /// address or a MagicDNS name. A LAN, loopback or public IP is refused:
    /// the router would not be reached over WireGuard, and could not tell
    /// who this machine is.
    ///
    /// The second value is a warning for a MagicDNS name, which resolves
    /// only while MagicDNS is on and could name something else off the
    /// tailnet. The tailnet IP is the firmer pin.
    pub fn otw_connect_url(&self) -> Result<(String, Option<String>)> {
        let base = self.url.trim_end_matches('/');
        let parsed = url::Url::parse(base).with_context(|| {
            format!(
                "server.url {base:?} is not a URL. In otw mode it is \
                 http://<tailnet address>:<port>"
            )
        })?;
        let scheme = match parsed.scheme() {
            "http" => "ws",
            "https" => "wss",
            _ => bail!("server.url must start with http:// or https://"),
        };
        let warning = match parsed.host() {
            Some(url::Host::Ipv4(ip)) if is_tailnet_ip(ip.into()) => None,
            Some(url::Host::Ipv6(ip)) if is_tailnet_ip(ip.into()) => None,
            Some(url::Host::Domain(name)) if is_magic_dns_name(name) => Some(format!(
                "server.url names {name}, a MagicDNS name: it resolves only while MagicDNS is \
                 on, so pinning the router's tailnet IP (`tailscale ip -4` on it) is firmer"
            )),
            Some(host) => bail!(
                "server.url names {host}, which is not on a tailnet. In otw mode the router is \
                 reached over Tailscale: use its tailnet IP (100.64.0.0/10 or \
                 fd7a:115c:a1e0::/48) or its MagicDNS name (<machine> or \
                 <machine>.<tailnet>.ts.net)"
            ),
            None => bail!("server.url {base:?} has no host"),
        };
        let rest = base
            .split_once("://")
            .map(|(_, rest)| rest)
            .unwrap_or_default();
        Ok((format!("{scheme}://{rest}/v1/connect"), warning))
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

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowConfig {
    #[serde(default)]
    pub repos: Vec<RepoSlug>,
    #[serde(default)]
    pub paths: Vec<PathBuf>,
}

fn default_integration_branch() -> String {
    "dev".to_owned()
}

fn default_event_interval_secs() -> u64 {
    120
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
        let user = Self::user_path()
            .ok_or_else(|| anyhow!("no home directory to resolve the config path against"))?;
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
        let mode = self.mode();
        if mode != Mode::Std {
            if self.allow.repos.is_empty() {
                bail!("allow.repos is empty, so nothing would ever be published");
            }
            if self.allow.paths.is_empty() {
                bail!("allow.paths is empty, so nothing would ever be published");
            }
        }
        if mode.has_remote_router() {
            self.router_url()?;
        }
        self.validate_rooms()?;
        self.validate_agents()?;
        crate::collect::generic::validate(&self.provider)?;
        self.events.validate()?;
        self.declared.validate()?;
        Ok(())
    }

    pub fn mode(&self) -> Mode {
        self.mesh.mode
    }

    /// The remote router to reach. An error in `std` mode, where the router
    /// runs inside the daemon and `[server]` is ignored, and when a mode that
    /// needs one has none.
    pub fn server(&self) -> Result<&ServerConfig> {
        let mode = self.mode();
        if !mode.has_remote_router() {
            bail!(
                "mode = \"std\" runs the router inside the daemon on this machine, so there is \
                 no remote [server] to reach"
            );
        }
        self.server.as_ref().ok_or_else(|| {
            anyhow!("mode = \"{mode}\" reaches a router elsewhere, so [server] url is required")
        })
    }

    /// The WebSocket URL of the remote router, checked the way this mode
    /// needs: cloud refuses plaintext off this machine, otw refuses a host
    /// that is not on the tailnet. An error in std mode, as [`Self::server`].
    pub fn router_url(&self) -> Result<String> {
        match self.mode() {
            Mode::Otw => Ok(self.server()?.otw_connect_url()?.0),
            Mode::Std | Mode::Cloud => self.server()?.connect_url(),
        }
    }

    /// Things about the config that work but deserve a second look, for
    /// `sealmesh doctor`. Never printed on load: every command loads the
    /// config, and the hook's stderr is not the place for them.
    pub fn warnings(&self) -> Vec<String> {
        if self.mode() != Mode::Otw {
            return Vec::new();
        }
        self.server
            .as_ref()
            .and_then(|server| server.otw_connect_url().ok())
            .and_then(|(_, warning)| warning)
            .into_iter()
            .collect()
    }

    /// Where this machine's handshake credential comes from, by mode.
    pub fn credential_source(&self) -> Result<crate::credential::CredentialSource> {
        crate::credential::CredentialSource::for_mode(self.mode())
    }

    /// The `[[room]]` blocks as the embedded router's table, for
    /// `ProjectMap::with_named_rooms`.
    #[cfg(feature = "std-router")]
    pub fn named_rooms(&self) -> sealmesh_server::named_rooms::NamedRooms {
        self.rooms
            .iter()
            .map(|room| (room.name.clone(), room.members.clone()))
            .collect()
    }

    fn validate_rooms(&self) -> Result<()> {
        if self.rooms.is_empty() {
            return Ok(());
        }
        if self.mode() == Mode::Cloud {
            bail!(
                "[[room]] is set, but named rooms exist only in std and otw mode. A cloud \
                 router decides who may join a room from GitHub; remove the [[room]] blocks or \
                 set [mesh] mode"
            );
        }
        let mut seen = BTreeSet::new();
        for room in &self.rooms {
            if !seen.insert(&room.name) {
                bail!("[[room]] {} is declared twice", room.name);
            }
            if room.members.is_empty() {
                bail!(
                    "[[room]] {} lists no members, so nobody could join it",
                    room.name
                );
            }
        }
        Ok(())
    }

    fn validate_agents(&self) -> Result<()> {
        if self.agents.is_empty() {
            return Ok(());
        }
        if self.mode() != Mode::Std {
            bail!(
                "[[agent]] is set, but agents.sock exists only in std mode, where the router \
                 runs inside the daemon on this machine; remove the [[agent]] blocks or set \
                 [mesh] mode = \"std\""
            );
        }
        let mut seen = BTreeSet::new();
        for agent in &self.agents {
            if agent.name.as_str() == BUILT_IN_AGENT {
                bail!(
                    "[[agent]] {BUILT_IN_AGENT} is built in: it is Claude, reaching rooms \
                     through the MCP tools. Name the agent something else"
                );
            }
            if !seen.insert(&agent.name) {
                bail!("[[agent]] {} is declared twice", agent.name);
            }
            for name in agent.rooms.iter().flatten() {
                let listed = self
                    .rooms
                    .iter()
                    .find(|room| &room.name == name)
                    .is_some_and(|room| room.members.contains(&agent.name));
                if !listed {
                    bail!(
                        "[[agent]] {} allows room {name}, but no [[room]] {name} lists {} as a \
                         member. An allowlist only narrows membership; add it to that room's \
                         members first",
                        agent.name,
                        agent.name
                    );
                }
            }
        }
        Ok(())
    }

    /// The named rooms `agent` may use: every `[[room]]` that lists it as a
    /// member, narrowed to its `rooms` allowlist when it has one. For
    /// [`BUILT_IN_AGENT`], every room that lists `claude`. In config order.
    pub fn agent_rooms(&self, agent: &sealmesh_wire::HostId) -> Vec<sealmesh_chat_wire::RoomName> {
        let allowlist = self
            .agents
            .iter()
            .find(|configured| &configured.name == agent)
            .and_then(|configured| configured.rooms.as_ref());
        self.rooms
            .iter()
            .filter(|room| room.members.contains(agent))
            .filter(|room| allowlist.is_none_or(|allowed| allowed.contains(&room.name)))
            .map(|room| room.name.clone())
            .collect()
    }

    /// The members `[[room]] name` lists, or `None` for a room not configured.
    pub fn room_members(
        &self,
        name: &sealmesh_chat_wire::RoomName,
    ) -> Option<&[sealmesh_wire::HostId]> {
        self.rooms
            .iter()
            .find(|room| &room.name == name)
            .map(|room| room.members.as_slice())
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

    /// The machine name this daemon tells the router: `machine` from the
    /// config, else the OS hostname folded into one (see
    /// [`MachineName::from_hostname`](sealmesh_proto::MachineName::from_hostname)).
    /// `None` only when neither gives one.
    pub fn machine_name(&self) -> Option<sealmesh_proto::MachineName> {
        self.machine.clone().or_else(|| {
            sysinfo::System::host_name()
                .and_then(|hostname| sealmesh_proto::MachineName::from_hostname(&hostname))
        })
    }

    /// Where [`Self::load`] looks when no `--config` is given:
    /// `$XDG_CONFIG_HOME/sealmesh/config.toml`.
    pub fn user_path() -> Option<PathBuf> {
        config_home().map(|d| d.join(USER_CONFIG))
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

    fn with_machine(line: &str) -> Result<Config, toml::de::Error> {
        toml::from_str(&format!(
            r#"
            ticket_prefixes = ["ENG"]
            {line}
            [server]
            url = "https://router.example.com"
            [allow]
            repos = ["example-org/example-repo"]
            paths = ["/tmp"]
            "#
        ))
    }

    #[test]
    fn a_configured_machine_name_wins_and_a_bad_one_is_refused() {
        let named = with_machine(r#"machine = "work-laptop""#).unwrap();
        assert_eq!(
            named.machine_name().unwrap().as_str(),
            "work-laptop",
            "the config's name, not the hostname"
        );
        // Not folded the way a hostname is: a name someone typed and got
        // wrong is a startup error, not a quietly different name.
        assert!(with_machine(r#"machine = "Work Laptop""#).is_err());
        assert!(with_machine(r#"machine = """#).is_err());
        assert!(with_machine("").unwrap().machine.is_none());
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
        assert!(
            !config.events.enabled,
            "events are deprecated and off by default"
        );
        assert_eq!(config.events.interval_secs, 120);
        let default = EventsConfig::default();
        assert!(!default.enabled, "Default and serde disagree on events");
        assert_eq!(default.interval_secs, config.events.interval_secs);
    }

    #[test]
    fn events_can_still_be_turned_on_explicitly() {
        let config = config_with("[events]\nenabled = true").unwrap();
        assert!(config.events.enabled);
        assert_eq!(config.events.interval_secs, 120);
    }

    #[test]
    fn a_config_with_no_ticket_prefixes_loads_and_validates() {
        let toml = r#"
            [server]
            url = "https://router.example.com"
            [allow]
            repos = ["example-org/example-repo"]
            paths = ["/tmp"]
        "#;
        let config: Config = toml::from_str(toml).unwrap();
        config.validate().unwrap();
        assert!(config.ticket_prefixes.is_empty());
        assert!(config.collect_options().ticket_prefixes.is_empty());

        let explicit_empty = format!("ticket_prefixes = []\n{toml}");
        let config: Config = toml::from_str(&explicit_empty).unwrap();
        config.validate().unwrap();
        assert!(config.ticket_prefixes.is_empty());
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

    fn std_config(body: &str) -> Result<Config> {
        let config: Config = toml::from_str(&format!("[mesh]\nmode = \"std\"\n{body}"))?;
        config.validate()?;
        Ok(config)
    }

    #[test]
    fn a_config_without_mesh_is_cloud_and_needs_what_it_always_needed() {
        let config = config_with("").unwrap();
        assert_eq!(config.mode(), Mode::Cloud);
        assert_eq!(
            config.server().unwrap().connect_url().unwrap(),
            "wss://router.example.com/v1/connect"
        );
        assert_eq!(
            config.credential_source().unwrap(),
            crate::credential::CredentialSource::GhCli
        );

        for missing in [
            // No [server].
            "[allow]\nrepos = [\"o/r\"]\npaths = [\"/tmp\"]",
            // No [allow].
            "[server]\nurl = \"https://router.example.com\"",
            // Empty [allow].
            "[server]\nurl = \"https://router.example.com\"\n[allow]\nrepos = []\npaths = []",
        ] {
            let config: Config = toml::from_str(missing).unwrap();
            assert!(config.validate().is_err(), "cloud accepted {missing}");
        }
        let explicit: Config = toml::from_str(
            "[mesh]\nmode = \"cloud\"\n[allow]\nrepos = [\"o/r\"]\npaths = [\"/tmp\"]",
        )
        .unwrap();
        let error = explicit.validate().unwrap_err();
        assert!(
            format!("{error:#}").contains("[server] url is required"),
            "{error:#}"
        );
    }

    #[test]
    fn a_std_config_needs_no_server_and_no_allow_list() {
        let bare = std_config("").unwrap();
        assert_eq!(bare.mode(), Mode::Std);
        assert!(bare.server().is_err(), "std mode has no remote router");
        assert!(bare.allow.repos.is_empty() && bare.allow.paths.is_empty());
        assert!(bare.gate().unwrap().paths().is_empty());
        assert!(matches!(
            bare.credential_source().unwrap(),
            crate::credential::CredentialSource::LocalSecret(_)
        ));

        // An empty [allow], and a [server] that is there but ignored, even
        // one that would be refused in cloud mode.
        let ignored = std_config(
            "[server]\nurl = \"http://router.example.com\"\n[allow]\nrepos = []\npaths = []",
        )
        .unwrap();
        assert!(ignored.server().is_err());
    }

    fn otw_config(url: &str) -> Result<Config> {
        let config: Config = toml::from_str(&format!(
            "[mesh]\nmode = \"otw\"\n[server]\nurl = \"{url}\"\n\
             [allow]\nrepos = [\"o/r\"]\npaths = [\"/tmp\"]"
        ))?;
        config.validate()?;
        Ok(config)
    }

    #[test]
    fn otw_reaches_its_router_by_tailnet_ip_in_plaintext() {
        for (url, socket) in [
            (
                "http://100.101.102.103:8787",
                "ws://100.101.102.103:8787/v1/connect",
            ),
            ("http://100.64.0.1:8787/", "ws://100.64.0.1:8787/v1/connect"),
            (
                "http://[fd7a:115c:a1e0::5]:8787",
                "ws://[fd7a:115c:a1e0::5]:8787/v1/connect",
            ),
            (
                "https://100.101.102.103",
                "wss://100.101.102.103/v1/connect",
            ),
        ] {
            let config = otw_config(url).unwrap();
            assert_eq!(config.router_url().unwrap(), socket, "{url}");
            assert!(config.warnings().is_empty(), "{url}");
        }
    }

    #[test]
    fn otw_warns_on_a_magic_dns_name_but_accepts_it() {
        for url in ["http://router:8787", "https://router.tail1234.ts.net"] {
            let config = otw_config(url).unwrap();
            assert!(config.router_url().is_ok(), "{url}");
            let warnings = config.warnings();
            assert_eq!(warnings.len(), 1, "{url}");
            assert!(
                warnings.iter().all(|w| w.contains("MagicDNS")),
                "{warnings:?}"
            );
        }
    }

    #[test]
    fn otw_refuses_a_router_url_off_the_tailnet() {
        for url in [
            "http://203.0.113.7:8787",
            "https://8.8.8.8",
            "http://192.168.1.20:8787",
            "http://127.0.0.1:8787",
            "http://[2001:db8::1]:8787",
            "https://router.example.com",
            "ftp://100.101.102.103",
        ] {
            let refused = otw_config(url);
            assert!(refused.is_err(), "accepted {url}");
        }
        let message = format!("{:#}", otw_config("http://203.0.113.7:8787").unwrap_err());
        assert!(message.contains("not on a tailnet"), "{message}");
    }

    #[test]
    fn cloud_keeps_its_own_url_rules() {
        // A tailnet IP over http is still plaintext off this machine in cloud
        // mode, where the handshake carries a GitHub token.
        let config: Config = toml::from_str(
            "[server]\nurl = \"http://100.101.102.103:8787\"\n\
             [allow]\nrepos = [\"o/r\"]\npaths = [\"/tmp\"]",
        )
        .unwrap();
        assert!(config.validate().is_err());
        assert!(config_with("").unwrap().warnings().is_empty());
    }

    #[test]
    fn otw_still_needs_its_router_url() {
        let config: Config = toml::from_str(
            "[mesh]\nmode = \"otw\"\n[allow]\nrepos = [\"o/r\"]\npaths = [\"/tmp\"]",
        )
        .unwrap();
        assert!(config.validate().is_err());
        let config = otw_config("http://100.101.102.103:8787").unwrap();
        assert_eq!(config.mode(), Mode::Otw);
        assert_eq!(
            config.credential_source().unwrap(),
            crate::credential::CredentialSource::None
        );
    }

    #[test]
    fn an_unknown_mode_or_mesh_key_is_refused() {
        assert!(toml::from_str::<Config>("[mesh]\nmode = \"lan\"").is_err());
        assert!(toml::from_str::<Config>("[mesh]\nmode = \"STD\"").is_err());
        assert!(toml::from_str::<Config>("[mesh]\nmode = \"std\"\nport = 1").is_err());
    }

    const OPS: &str = "[[room]]\nname = \"ops\"\nmembers = [\"claude\", \"grokbot\"]";

    #[test]
    fn named_rooms_load_in_std_and_otw_mode() {
        let config = std_config(OPS).unwrap();
        assert_eq!(config.rooms.len(), 1);
        let ops = config.rooms.first().unwrap();
        assert_eq!(ops.name.as_str(), "ops");
        assert_eq!(ops.members.len(), 2);
        let otw: Config = toml::from_str(&format!(
            "[mesh]\nmode = \"otw\"\n[server]\nurl = \"http://100.101.102.103:8787\"\n\
             [allow]\nrepos = [\"o/r\"]\npaths = [\"/tmp\"]\n{OPS}"
        ))
        .unwrap();
        otw.validate().unwrap();
        assert_eq!(otw.rooms.len(), 1);
    }

    #[cfg(feature = "std-router")]
    #[test]
    fn named_rooms_become_the_routers_table() {
        let table = std_config(OPS).unwrap().named_rooms();
        let ops = sealmesh_chat_wire::RoomName::try_from("ops").unwrap();
        let claude = sealmesh_wire::HostId::try_from("claude").unwrap();
        let mallory = sealmesh_wire::HostId::try_from("mallory").unwrap();
        assert!(table.admits(&ops, &claude));
        assert!(!table.admits(&ops, &mallory));
        assert!(std_config("").unwrap().named_rooms().is_empty());
    }

    #[test]
    fn cloud_mode_refuses_named_rooms_with_a_clear_error() {
        let error = config_with(OPS).unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("std and otw mode"), "{message}");
    }

    #[test]
    fn a_bad_named_room_is_refused_at_load() {
        let twice = format!("{OPS}\n{OPS}");
        for bad in [
            "[[room]]\nname = \"Ops\"\nmembers = [\"claude\"]",
            "[[room]]\nname = \"ops\"\nmembers = [\"Claude Code\"]",
            "[[room]]\nname = \"ops\"\nmembers = []",
            "[[room]]\nname = \"ops\"\nmembers = [\"claude\"]\nowner = \"me\"",
            twice.as_str(),
        ] {
            assert!(std_config(bad).is_err(), "accepted {bad}");
        }
    }

    const AGENTS: &str = "[[room]]\nname = \"ops\"\nmembers = [\"claude\", \"grokbot\", \"pagerbot\"]\n\
         [[room]]\nname = \"dev\"\nmembers = [\"claude\", \"grokbot\"]\n\
         [[agent]]\nname = \"grokbot\"\n\
         [[agent]]\nname = \"pagerbot\"\nrooms = [\"ops\"]";

    fn host(name: &str) -> sealmesh_wire::HostId {
        sealmesh_wire::HostId::try_from(name).unwrap()
    }

    fn names(rooms: &[sealmesh_chat_wire::RoomName]) -> Vec<&str> {
        rooms
            .iter()
            .map(sealmesh_chat_wire::RoomName::as_str)
            .collect()
    }

    #[test]
    fn an_agent_uses_the_rooms_that_list_it_narrowed_by_its_allowlist() {
        let config = std_config(AGENTS).unwrap();
        assert_eq!(config.agents.len(), 2);
        assert_eq!(names(&config.agent_rooms(&host("grokbot"))), ["ops", "dev"]);
        assert_eq!(names(&config.agent_rooms(&host("pagerbot"))), ["ops"]);
        assert_eq!(names(&config.agent_rooms(&host("claude"))), ["ops", "dev"]);
        assert!(config.agent_rooms(&host("mallory")).is_empty());
    }

    #[test]
    fn agents_exist_only_in_std_mode() {
        let agent = "[[agent]]\nname = \"grokbot\"";
        let error = config_with(agent).unwrap_err();
        assert!(format!("{error:#}").contains("std mode"), "{error:#}");
        assert!(config_with(&format!("[mesh]\nmode = \"otw\"\n{agent}")).is_err());
        assert!(std_config(agent).is_ok());
    }

    #[test]
    fn a_bad_agent_is_refused_at_load() {
        for bad in [
            // The built-in agent's name.
            "[[agent]]\nname = \"claude\"",
            // Declared twice.
            "[[agent]]\nname = \"grokbot\"\n[[agent]]\nname = \"grokbot\"",
            // Not a name.
            "[[agent]]\nname = \"Grok Bot\"",
            "[[agent]]\nname = \"../grokbot\"",
            // A field an agent does not have: identity is never configured
            // by the agent, nor an author set.
            "[[agent]]\nname = \"grokbot\"\nauthor = \"claude\"",
            // An allowlist naming a room that does not list it.
            "[[room]]\nname = \"ops\"\nmembers = [\"claude\"]\n[[agent]]\nname = \"grokbot\"\nrooms = [\"ops\"]",
            // An allowlist naming a room that does not exist.
            "[[agent]]\nname = \"grokbot\"\nrooms = [\"ghost\"]",
        ] {
            assert!(std_config(bad).is_err(), "accepted {bad}");
        }
    }

    #[cfg(feature = "std-router")]
    #[test]
    fn the_built_in_agent_is_the_name_the_router_gives_the_daemon() {
        assert_eq!(BUILT_IN_AGENT, sealmesh_server::local_owner::BUILT_IN_AGENT);
    }
}
