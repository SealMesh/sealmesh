//! `[mesh] mode = "std"`: one machine, no network.
//!
//! `sealmesh daemon` starts the router in its own process, over a
//! `LocalOwner` directory (whoever reads the per-boot secret is this OS user)
//! with the config's `[[room]]` table and SQLite history kept 7 days. It
//! listens on a private Unix socket and nothing else: no TCP listener exists
//! in this mode. The daemon then connects to it through `client::connect`
//! over that socket, like any other router, so `who`, the MCP tools and
//! `sealmesh chat` work unchanged.
//!
//! Beside it, `agents.sock` seats each `[[agent]]` in the same router under
//! its own name (see `agent_link`).

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use sealmesh_server::{
    Listener, history::sqlite::SqliteHistory, http::App, local_owner::LocalOwner,
    mode_directory::ModeDirectory, projects::ProjectMap,
};
use sealmesh_wire::HostId;
use tokio::task::JoinHandle;

use crate::{
    agent_link::{
        link::{self, Link},
        seat::Seat,
        tokens::Tokens,
    },
    config::Config,
    credential,
    room::{Gate, socket},
};

/// How often expired history rows are deleted. Expired rows are already
/// invisible to every read; this only reclaims the space.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Every path std mode uses, so a test can put them all under a temporary
/// directory and the daemon under `$XDG_STATE_HOME`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    pub router_secret: PathBuf,
    pub router_socket: PathBuf,
    pub history: PathBuf,
    pub tokens_dir: PathBuf,
    pub agents_socket: PathBuf,
    pub audit: Option<PathBuf>,
}

impl Layout {
    /// The layout under one state directory (`$XDG_STATE_HOME`).
    pub fn under(state: &Path) -> Self {
        let root = state.join("sealmesh");
        let router = root.join("router");
        Self {
            router_secret: router.join("secret"),
            router_socket: router.join("router.sock"),
            history: router.join("history.sqlite"),
            tokens_dir: root.join("agents"),
            agents_socket: root.join("agents.sock"),
            audit: Some(root.join("audit.jsonl")),
        }
    }

    /// The layout the daemon runs with: under `$XDG_STATE_HOME`, or
    /// `~/.local/state`.
    pub fn from_env() -> Result<Self> {
        let state = crate::paths::state_home()
            .ok_or_else(|| anyhow!("no home directory to keep the local router's state under"))?;
        Ok(Self::under(&state))
    }
}

/// The embedded router and agent link, running. Dropping it stops both.
pub struct Embedded {
    pub app: Arc<App<ModeDirectory>>,
    /// Where agents connect, when any `[[agent]]` is configured.
    pub agents_socket: Option<PathBuf>,
    pub link_stats: Arc<link::Stats>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Embedded {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// Starts the router on `layout.router_socket` and, when agents are
/// configured, the agent link on `layout.agents_socket`. `gate` is chat's
/// switch: the link answers `disabled` while it is off.
pub async fn start(config: &Config, layout: &Layout, gate: Gate) -> Result<Embedded> {
    socket::check_length(&layout.router_socket)?;
    let owner = LocalOwner::boot(&layout.router_secret)?;
    // The seats act for the daemon, the router's one member, so they hold
    // its secret: read back exactly as the daemon's own connection reads it.
    let secret = credential::read_local_secret(&layout.router_secret)?;
    let history = SqliteHistory::open(&layout.history)?;
    history.spawn_sweeper(SWEEP_INTERVAL);
    let projects = ProjectMap::empty().with_named_rooms(config.named_rooms());
    let app = Arc::new(App::new(
        ModeDirectory::from(owner),
        projects,
        Arc::new(history),
    ));

    let (listener, uid) = listen(&layout.router_socket)?;
    let router_app = Arc::clone(&app);
    let mut tasks = vec![tokio::spawn(async move {
        if let Err(error) = sealmesh_server::run(router_app, Listener::Unix(listener)).await {
            eprintln!("the embedded router stopped: {error:#}");
        }
    })];

    let names: Vec<HostId> = config
        .agents
        .iter()
        .map(|agent| agent.name.clone())
        .collect();
    // Always synced, even with no agents left, so a removed agent's token is
    // deleted on the next start.
    let (tokens, synced) = Tokens::sync(&layout.tokens_dir, &names, uid)?;
    for name in &synced.created {
        eprintln!(
            "agent {name}: token written to {}",
            crate::agent_link::tokens::token_path(&layout.tokens_dir, name).display()
        );
    }
    for name in &synced.replaced {
        eprintln!(
            "agent {name}: its token file was readable by others or malformed, so it was \
             replaced; hand the bot the new one"
        );
    }
    for name in &synced.revoked {
        eprintln!("agent {name}: no longer configured, its token was deleted");
    }

    let link_stats = Arc::new(link::Stats::default());
    let agents_socket = if names.is_empty() {
        None
    } else {
        let seats = names
            .iter()
            .map(|name| {
                Seat::spawn(
                    Arc::clone(&app),
                    secret.clone(),
                    name.clone(),
                    config.agent_rooms(name),
                )
            })
            .collect();
        let members: BTreeMap<_, _> = config
            .rooms
            .iter()
            .map(|room| (room.name.clone(), room.members.clone()))
            .collect();
        let link = Arc::new(Link::new(
            tokens,
            seats,
            members,
            gate,
            layout.audit.clone(),
        ));
        socket::check_length(&layout.agents_socket)?;
        let (listener, uid) = listen(&layout.agents_socket)?;
        let stats = Arc::clone(&link_stats);
        tasks.push(tokio::spawn(async move {
            if let Err(error) = link::serve(listener, uid, link, stats).await {
                eprintln!("agents.sock stopped: {error:#}");
            }
        }));
        Some(layout.agents_socket.clone())
    };

    eprintln!(
        "std mode: router on {} (no TCP listener), history in {} for 7 days{}",
        layout.router_socket.display(),
        layout.history.display(),
        agents_socket
            .as_ref()
            .map_or_else(String::new, |path| format!(
                ", {} agents on {}",
                names.len(),
                path.display()
            ))
    );

    Ok(Embedded {
        app,
        agents_socket,
        link_stats,
        tasks,
    })
}

/// Binds a private Unix socket (0600 in a 0700 directory) and hands it to
/// Tokio. Returns it and the uid every peer must have.
fn listen(path: &Path) -> Result<(tokio::net::UnixListener, u32)> {
    let (listener, uid) = socket::bind(path)?;
    listener
        .set_nonblocking(true)
        .with_context(|| format!("making {} non-blocking", path.display()))?;
    let listener = tokio::net::UnixListener::from_std(listener)
        .with_context(|| format!("registering {}", path.display()))?;
    Ok((listener, uid))
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
        io::{Read as _, Write as _},
        process::{Command, Stdio},
        sync::atomic::Ordering,
    };

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    /// A fresh directory with a short name: a Unix socket path is capped
    /// near 104 bytes.
    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sm{}{tag}", std::process::id() % 100_000));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn the_layout_is_the_one_the_rest_of_the_crate_reads() {
        let layout = Layout::from_env().unwrap();
        assert_eq!(
            Some(layout.router_secret),
            crate::paths::local_router_secret()
        );
        assert_eq!(
            Some(layout.router_socket),
            crate::paths::local_router_socket()
        );
        assert_eq!(Some(layout.history), crate::paths::local_router_history());
        assert_eq!(Some(layout.tokens_dir), crate::paths::agent_tokens_dir());
        assert_eq!(Some(layout.agents_socket), crate::paths::agents_socket());
        assert_eq!(layout.audit, crate::audit::path());
    }

    #[tokio::test]
    async fn agents_sock_refuses_a_peer_with_another_uid() {
        let root = scratch("u");
        let path = root.join("s").join("agents.sock");
        let (listener, uid) = listen(&path).unwrap();
        let token = "a".repeat(64);
        let link = Arc::new(Link::new(
            Tokens::fixed(&[("grokbot", &token)]),
            Vec::new(),
            BTreeMap::new(),
            Arc::new(|| true),
            None,
        ));
        let stats = Arc::new(link::Stats::default());
        // The server believes the socket belongs to a different uid, so this
        // process is the "other user".
        let task = tokio::spawn(link::serve(
            listener,
            uid.wrapping_add(1),
            link,
            Arc::clone(&stats),
        ));

        let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();
        let request = format!(
            "GET /v1/whoami HTTP/1.1\r\nHost: sealmesh\r\nAuthorization: Bearer {token}\r\n\
             Connection: close\r\n\r\n"
        );
        let _ = stream.write_all(request.as_bytes()).await;
        let mut answer = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut answer)).await;
        assert!(
            answer.is_empty(),
            "another uid got an answer: {}",
            String::from_utf8_lossy(&answer)
        );
        let started = std::time::Instant::now();
        while stats.refused_peer.load(Ordering::SeqCst) < 1 {
            assert!(started.elapsed() < Duration::from_secs(5), "never refused");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(stats.accepted.load(Ordering::SeqCst), 0);
        task.abort();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn both_sockets_are_private_files_in_private_directories() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = scratch("p");
        let layout = Layout::under(&root);
        let config: Config = toml::from_str(
            "[mesh]\nmode = \"std\"\n[[room]]\nname = \"ops\"\nmembers = [\"claude\", \"grokbot\"]\n\
             [[agent]]\nname = \"grokbot\"",
        )
        .unwrap();
        let embedded = start(&config, &layout, Arc::new(|| true)).await.unwrap();
        let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        for socket in [&layout.router_socket, &layout.agents_socket] {
            assert_eq!(mode(socket), 0o600, "{}", socket.display());
            assert_eq!(
                mode(socket.parent().unwrap()),
                0o700,
                "{}",
                socket.display()
            );
        }
        assert_eq!(mode(&layout.router_secret), 0o600);
        assert_eq!(mode(&layout.tokens_dir.join("grokbot.token")), 0o600);
        drop(embedded);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Run only by [`a_std_daemon_listens_on_no_tcp_port`], in a child
    /// process of its own, so no other test's listener shares its pid: the
    /// whole `sealmesh daemon`, from the config the parent wrote, under the
    /// parent's temporary XDG directories.
    #[tokio::test]
    #[ignore = "a child process of a_std_daemon_listens_on_no_tcp_port"]
    async fn std_daemon_child() {
        let Ok(path) = std::env::var("SEALMESH_STD_CHILD_CONFIG") else {
            return;
        };
        let config = Config::load(Some(Path::new(&path))).unwrap();
        crate::daemon::run(&config).await.unwrap();
    }

    /// Whether process `pid` has a TCP socket in the listening state.
    #[cfg(target_os = "linux")]
    fn listens_on_tcp(pid: u32) -> bool {
        let mut inodes = std::collections::BTreeSet::new();
        for entry in std::fs::read_dir(format!("/proc/{pid}/fd"))
            .unwrap()
            .flatten()
        {
            if let Ok(target) = std::fs::read_link(entry.path()) {
                let target = target.to_string_lossy().into_owned();
                if let Some(inode) = target
                    .strip_prefix("socket:[")
                    .and_then(|rest| rest.strip_suffix(']'))
                {
                    inodes.insert(inode.to_owned());
                }
            }
        }
        ["tcp", "tcp6"].iter().any(|table| {
            std::fs::read_to_string(format!("/proc/{pid}/net/{table}"))
                .unwrap_or_default()
                .lines()
                .skip(1)
                .any(|line| {
                    let fields: Vec<&str> = line.split_whitespace().collect();
                    fields.get(3) == Some(&"0A")
                        && fields.get(9).is_some_and(|inode| inodes.contains(*inode))
                })
        })
    }

    /// Whether process `pid` has a TCP socket in the listening state.
    #[cfg(not(target_os = "linux"))]
    fn listens_on_tcp(pid: u32) -> bool {
        let output = Command::new("lsof")
            .args([
                "-nP",
                "-a",
                "-p",
                &pid.to_string(),
                "-iTCP",
                "-sTCP:LISTEN",
                "-Fn",
            ])
            .output()
            .expect("lsof is needed to check for TCP listeners on this platform");
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line.starts_with('n'))
    }

    fn whoami(socket: &Path, token: &str) -> String {
        let mut stream = std::os::unix::net::UnixStream::connect(socket).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        write!(
            stream,
            "GET /v1/whoami HTTP/1.1\r\nHost: sealmesh\r\nAuthorization: Bearer {token}\r\n\
             Connection: close\r\n\r\n"
        )
        .unwrap();
        let mut answer = String::new();
        stream.read_to_string(&mut answer).unwrap();
        answer
    }

    #[test]
    fn a_std_daemon_listens_on_no_tcp_port() {
        // The probe itself, against a listener this process does have, so a
        // probe that sees nothing cannot pass for a daemon that has nothing.
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        assert!(
            listens_on_tcp(std::process::id()),
            "the probe cannot see a TCP listener"
        );
        drop(probe);

        let root = scratch("t");
        let state = root.join("s");
        let config = root.join("config.toml");
        std::fs::write(
            &config,
            "[mesh]\nmode = \"std\"\n[chat]\nenabled = true\n\
             [[room]]\nname = \"ops\"\nmembers = [\"claude\", \"grokbot\"]\n\
             [[agent]]\nname = \"grokbot\"\n",
        )
        .unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "std_mode::tests::std_daemon_child",
                "--ignored",
                "--test-threads=1",
            ])
            .env("SEALMESH_STD_CHILD_CONFIG", &config)
            .env("XDG_STATE_HOME", &state)
            .env("XDG_CONFIG_HOME", root.join("c"))
            .env("XDG_CACHE_HOME", root.join("k"))
            .env("HOME", root.join("h"))
            .env("CODEX_HOME", root.join("x"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        let layout = Layout::under(&state);
        let local = state.join("sealmesh/local/daemon.sock");
        let token_file = layout.tokens_dir.join("grokbot.token");
        let started = std::time::Instant::now();
        while !(token_file.exists()
            && std::os::unix::net::UnixStream::connect(&layout.agents_socket).is_ok()
            && std::os::unix::net::UnixStream::connect(&layout.router_socket).is_ok()
            && std::os::unix::net::UnixStream::connect(&local).is_ok())
        {
            if started.elapsed() > Duration::from_secs(30) {
                let _ = child.kill();
                let _ = child.wait();
                panic!("the std daemon never came up");
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        // It is up and serving its agent over a Unix socket.
        let token = std::fs::read_to_string(&token_file).unwrap();
        let answer = whoami(&layout.agents_socket, token.trim());
        let listening = listens_on_tcp(child.id());
        let _ = child.kill();
        let _ = child.wait();
        let _ = std::fs::remove_dir_all(&root);
        assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
        assert!(answer.contains(r#""agent":"grokbot""#), "{answer}");
        assert!(!listening, "a std daemon has a TCP listener");
    }
}
