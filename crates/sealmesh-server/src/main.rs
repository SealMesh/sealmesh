use std::{
    net::{SocketAddr, ToSocketAddrs as _},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context as _, Result, bail};
use sealmesh_server::{
    access_cache::{CachingDirectory, MAX_TTL as ACCESS_CACHE_MAX_TTL},
    check_budget::{CHECKS_PER_HOUR_VAR, CheckBudgets, checks_per_hour_from_env},
    config::{
        ACCESS_CACHE_TTL_VAR, CONFIG_VAR, HISTORY_BACKEND_VAR, HISTORY_PATH_VAR, LISTEN_VAR,
        ORG_VAR, ServerConfig,
    },
    github::Github,
    history::{self, RoomHistory, sqlite::SqliteHistory},
    http::{App, router},
    listen::{TLS_TERMINATOR_VAR, permit_plaintext},
    projects::{PROJECTS_VAR, ProjectMap},
    room_budget::{TOKENS_PER_HOUR_VAR, tokens_per_hour_from_env},
    rooms::Rooms,
    turn_cap::{TURN_CAP_VAR, turn_cap_from_env},
};

const DEFAULT_LISTEN: &str = "0.0.0.0:8787";

/// How long a repo-access answer stays cached. Not an authorization input in
/// the sense rule 8 means (it decides how long an already-resolved decision
/// is trusted, not who may connect), so a default is fine here — but the
/// value is still bounded, in code, by `access_cache::MAX_TTL` regardless of
/// what this reads.
const DEFAULT_ACCESS_CACHE_TTL_SECS: u64 = 30;

/// How often expired messages are deleted from a SQLite history. Expired rows
/// are already invisible to every read; this only reclaims the space.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// The command line: `sealmesh-server [--config PATH] [healthcheck]`.
#[derive(Debug, Default, PartialEq, Eq)]
struct Args {
    config: Option<PathBuf>,
    healthcheck: bool,
}

impl Args {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut parsed = Self::default();
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--config" => {
                    let path = args.next().context("--config needs a path")?;
                    parsed.config = Some(PathBuf::from(path));
                }
                "healthcheck" => parsed.healthcheck = true,
                other => bail!(
                    "unknown argument {other:?}. Usage: sealmesh-server [--config PATH] [healthcheck]"
                ),
            }
        }
        Ok(parsed)
    }
}

/// The config file from `--config`, else from `SEALMESH_CONFIG`, else none:
/// every setting then comes from its environment variable alone.
fn load_config(args: &Args) -> Result<ServerConfig> {
    let path = args
        .config
        .clone()
        .or_else(|| std::env::var_os(CONFIG_VAR).map(PathBuf::from));
    match path {
        Some(path) => ServerConfig::load(&path),
        None => Ok(ServerConfig::default()),
    }
}

fn listen_addr(config: &ServerConfig) -> String {
    config
        .setting(LISTEN_VAR)
        .unwrap_or_else(|_| DEFAULT_LISTEN.to_owned())
}

/// Reads the configured access-cache TTL, if any. A value above the hard
/// ceiling is refused rather than silently clamped, so a misconfiguration is
/// a startup error instead of a quietly different cache lifetime than the
/// operator asked for. `CachingDirectory::new` clamps too, as the ceiling
/// that holds regardless of this function.
fn access_cache_ttl(config: &ServerConfig) -> Result<Duration> {
    parse_access_cache_ttl(config.setting(ACCESS_CACHE_TTL_VAR).ok().as_deref())
}

/// The parsing logic behind [`access_cache_ttl`], pulled out so a test can
/// exercise it without mutating the real process environment (env vars are
/// global state shared by every test in this binary).
fn parse_access_cache_ttl(raw: Option<&str>) -> Result<Duration> {
    let Some(raw) = raw else {
        return Ok(Duration::from_secs(DEFAULT_ACCESS_CACHE_TTL_SECS));
    };
    let secs: u64 = raw.trim().parse().with_context(|| {
        format!("{ACCESS_CACHE_TTL_VAR} is not a whole number of seconds: {raw:?}")
    })?;
    let ttl = Duration::from_secs(secs);
    if ttl > ACCESS_CACHE_MAX_TTL {
        bail!(
            "{ACCESS_CACHE_TTL_VAR}={secs} exceeds the hard ceiling of {} seconds",
            ACCESS_CACHE_MAX_TTL.as_secs()
        );
    }
    Ok(ttl)
}

/// The SQLite file: required, never defaulted (rule 8's reasoning one step
/// over — a guessed path writes an org's coordination history somewhere nobody
/// is looking).
fn history_path(value: Result<String, std::env::VarError>) -> Result<PathBuf> {
    let raw = value.map_err(|_| {
        anyhow::anyhow!(
            "{HISTORY_PATH_VAR} (or [history] path) is not set, so room messages would have nowhere to go"
        )
    })?;
    let raw = raw.trim();
    if raw.is_empty() {
        bail!("{HISTORY_PATH_VAR} is empty, so room messages would have nowhere to go");
    }
    Ok(PathBuf::from(raw))
}

/// Opens the configured history store. Returns it and a description of where
/// messages go, for the startup line.
async fn open_history(config: &ServerConfig) -> Result<(Arc<dyn RoomHistory>, String)> {
    let backend = config
        .setting(HISTORY_BACKEND_VAR)
        .unwrap_or_else(|_| "sqlite".to_owned());
    match backend.trim() {
        "sqlite" => {
            let path = history_path(config.setting(HISTORY_PATH_VAR))?;
            let store = SqliteHistory::open(&path)?;
            spawn_sweeper(store.clone());
            Ok((Arc::new(store), format!("SQLite at {}", path.display())))
        }
        "dynamodb" => open_dynamodb(config).await,
        other => bail!("{HISTORY_BACKEND_VAR} must be \"sqlite\" or \"dynamodb\", not {other:?}"),
    }
}

/// Deletes expired SQLite rows on an interval, for as long as the router runs.
/// A failed sweep is logged and retried next time: reads never serve an
/// expired message either way.
fn spawn_sweeper(store: SqliteHistory) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SWEEP_INTERVAL);
        loop {
            tick.tick().await;
            let swept = match history::now_epoch() {
                Ok(now) => store.sweep_expired(now).await,
                Err(error) => Err(error),
            };
            if let Err(error) = swept {
                eprintln!("sweeping expired room messages failed: {error}");
            }
        }
    });
}

#[cfg(feature = "dynamodb")]
async fn open_dynamodb(config: &ServerConfig) -> Result<(Arc<dyn RoomHistory>, String)> {
    // Required with no default, for the same reason as the SQLite path.
    let table = history::table_from_env(config.setting(history::TABLE_VAR))?;
    // Region and credentials come from the SDK's default chain. Nothing about
    // them is configured here, and no credential is read, stored or logged by
    // this process.
    let aws = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .load()
        .await;
    let store = history::Dynamo::new(aws_sdk_dynamodb::Client::new(&aws), table.clone());
    Ok((Arc::new(store), format!("DynamoDB table {table}")))
}

#[cfg(not(feature = "dynamodb"))]
async fn open_dynamodb(_config: &ServerConfig) -> Result<(Arc<dyn RoomHistory>, String)> {
    bail!(
        "{HISTORY_BACKEND_VAR} is \"dynamodb\", but this build has no DynamoDB support. \
         Use \"sqlite\", or build sealmesh-server with --features dynamodb"
    )
}

fn probe_url(listen: &str) -> Result<String> {
    let addr = listen
        .to_socket_addrs()
        .with_context(|| format!("{LISTEN_VAR} is not an address to bind: {listen}"))?
        .next()
        .with_context(|| format!("{LISTEN_VAR} resolved to no address: {listen}"))?;

    let host = match (addr, addr.ip().is_unspecified()) {
        (SocketAddr::V4(_), true) => "127.0.0.1".to_owned(),
        (SocketAddr::V6(_), true) => "[::1]".to_owned(),
        (SocketAddr::V4(a), false) => a.ip().to_string(),
        (SocketAddr::V6(a), false) => format!("[{}]", a.ip()),
    };
    Ok(format!("http://{host}:{}/v1/healthz", addr.port()))
}

async fn healthcheck(config: &ServerConfig) -> Result<()> {
    let url = probe_url(&listen_addr(config))?;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .context("building the probe client")?;
    let status = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("probing {url}"))?
        .status();
    if status.is_success() {
        Ok(())
    } else {
        bail!("{url} answered {status}")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse(std::env::args().skip(1))?;
    let config = load_config(&args)?;
    if args.healthcheck {
        return healthcheck(&config).await;
    }

    let org = config
        .setting(ORG_VAR)
        .with_context(|| format!("{ORG_VAR} is not set, so nobody could be authorised"))?;
    if org.trim().is_empty() {
        bail!("{ORG_VAR} is empty, so nobody could be authorised");
    }

    let projects = config.setting(PROJECTS_VAR).with_context(|| {
        format!("{PROJECTS_VAR} is not set, so no repo could be placed in a project")
    })?;
    let projects = ProjectMap::parse(&projects).with_context(|| {
        format!("{PROJECTS_VAR} is invalid, refusing to start with part of a project map")
    })?;
    let project_count = projects.len();

    let access_cache_ttl = access_cache_ttl(&config)?;
    // Not an authorization input, so it may default (rule 8). An out-of-range
    // or malformed value still refuses to start rather than being clamped.
    let room_tokens = tokens_per_hour_from_env(config.setting(TOKENS_PER_HOUR_VAR))?;
    // The same: it bounds how long two hosts may talk alone, not who gets in.
    let turn_cap = turn_cap_from_env(config.setting(TURN_CAP_VAR))?;
    // The same: it bounds how much of a reader's GitHub rate limit other
    // machines' answers can spend, not who may read. A refused check withholds.
    let reader_checks = checks_per_hour_from_env(config.setting(CHECKS_PER_HOUR_VAR))?;
    let (history, recorded_in) = open_history(&config).await?;

    let github = Github::new(org.clone()).context("building the GitHub client")?;
    let directory = CachingDirectory::new(github, access_cache_ttl);
    let rooms = Rooms::with_limits(room_tokens, turn_cap, history);
    let mut app = App::with_rooms(directory, projects, rooms);
    app.check_budgets = CheckBudgets::new(reader_checks);
    let app = Arc::new(app);
    let router = router(app);

    let addr = listen_addr(&config);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding {addr}"))?;

    let bound = listener
        .local_addr()
        .context("reading back the address that was bound")?;
    let terminator = config.setting(TLS_TERMINATOR_VAR).ok();
    permit_plaintext(bound, terminator.as_deref())?;

    let retention_days = history::RETENTION.as_secs() / (24 * 60 * 60);
    match terminator.as_deref() {
        Some(name) => eprintln!(
            "sealmesh-server listening on {bound} behind TLS terminated by {name}, authorising members of {org}, {project_count} projects mapped, {} room tokens per host per room per hour, {} uncached access checks per reader per hour, room messages recorded in {recorded_in} for {retention_days} days",
            room_tokens.get(),
            reader_checks.get()
        ),
        None => eprintln!(
            "sealmesh-server listening on {bound} (loopback, plaintext), authorising members of {org}, {project_count} projects mapped, {} room tokens per host per room per hour, {} uncached access checks per reader per hour, room messages recorded in {recorded_in} for {retention_days} days",
            room_tokens.get(),
            reader_checks.get()
        ),
    }
    axum::serve(listener, router).await.context("serving")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn a_wildcard_bind_is_probed_on_loopback() {
        assert_eq!(
            probe_url(DEFAULT_LISTEN).unwrap(),
            "http://127.0.0.1:8787/v1/healthz"
        );
        assert_eq!(
            probe_url("[::]:8787").unwrap(),
            "http://[::1]:8787/v1/healthz"
        );
    }

    #[test]
    fn a_non_default_port_is_probed_on_that_port() {
        assert_eq!(
            probe_url("0.0.0.0:9000").unwrap(),
            "http://127.0.0.1:9000/v1/healthz"
        );
    }

    #[test]
    fn a_specific_bind_is_probed_where_it_bound() {
        assert_eq!(
            probe_url("10.0.4.17:8787").unwrap(),
            "http://10.0.4.17:8787/v1/healthz"
        );
        assert_eq!(
            probe_url("127.0.0.1:8787").unwrap(),
            "http://127.0.0.1:8787/v1/healthz"
        );
    }

    #[test]
    fn an_unusable_listen_address_is_an_error_not_a_guess() {
        let error = probe_url("not an address").unwrap_err().to_string();
        assert!(error.contains(LISTEN_VAR), "{error}");
    }

    #[test]
    fn an_unset_access_cache_ttl_falls_back_to_the_short_default() {
        assert_eq!(
            parse_access_cache_ttl(None).unwrap(),
            Duration::from_secs(DEFAULT_ACCESS_CACHE_TTL_SECS)
        );
    }

    #[test]
    fn a_configured_access_cache_ttl_under_the_ceiling_is_used() {
        assert_eq!(
            parse_access_cache_ttl(Some("5")).unwrap(),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn a_configured_access_cache_ttl_of_zero_is_accepted() {
        // Not the default (an unset variable falls back to the short
        // default), but a legal explicit value — an operator asking for no
        // caching at all is not a value above the ceiling and should not be
        // refused as one.
        assert_eq!(
            parse_access_cache_ttl(Some("0")).unwrap(),
            Duration::from_secs(0)
        );
    }

    #[test]
    fn a_negative_access_cache_ttl_is_refused_not_wrapped() {
        // `u64::parse` rejects a leading `-` outright, so this exercises the
        // same refusal path as non-numeric input rather than, say, silently
        // wrapping to a huge unsigned value.
        let error = parse_access_cache_ttl(Some("-1")).unwrap_err().to_string();
        assert!(error.contains(ACCESS_CACHE_TTL_VAR), "{error}");
    }

    #[test]
    fn a_configured_access_cache_ttl_above_the_ceiling_is_refused() {
        let over = ACCESS_CACHE_MAX_TTL.as_secs() + 1;
        let error = parse_access_cache_ttl(Some(&over.to_string()))
            .unwrap_err()
            .to_string();
        assert!(error.contains(ACCESS_CACHE_TTL_VAR), "{error}");
    }

    #[test]
    fn a_configured_access_cache_ttl_at_the_ceiling_is_accepted() {
        assert_eq!(
            parse_access_cache_ttl(Some(&ACCESS_CACHE_MAX_TTL.as_secs().to_string())).unwrap(),
            ACCESS_CACHE_MAX_TTL
        );
    }

    #[test]
    fn a_non_numeric_access_cache_ttl_is_an_error_not_a_silent_default() {
        let error = parse_access_cache_ttl(Some("soon"))
            .unwrap_err()
            .to_string();
        assert!(error.contains(ACCESS_CACHE_TTL_VAR), "{error}");
    }

    fn args(list: &[&str]) -> Result<Args> {
        Args::parse(list.iter().map(|s| (*s).to_owned()))
    }

    #[test]
    fn the_command_line_takes_a_config_path_and_healthcheck_in_any_order() {
        assert_eq!(args(&[]).unwrap(), Args::default());
        let both = Args {
            config: Some(PathBuf::from("/etc/sealmesh-server.toml")),
            healthcheck: true,
        };
        assert_eq!(
            args(&["--config", "/etc/sealmesh-server.toml", "healthcheck"]).unwrap(),
            both
        );
        assert_eq!(
            args(&["healthcheck", "--config", "/etc/sealmesh-server.toml"]).unwrap(),
            both
        );
    }

    #[test]
    fn a_bad_command_line_is_refused_not_guessed() {
        assert!(args(&["--config"]).is_err());
        assert!(args(&["--verbose"]).is_err());
    }

    #[test]
    fn the_history_path_is_required_and_never_defaulted() {
        let missing = history_path(Err(std::env::VarError::NotPresent))
            .unwrap_err()
            .to_string();
        assert!(missing.contains(HISTORY_PATH_VAR), "{missing}");
        assert!(history_path(Ok("  ".to_owned())).is_err());
        assert_eq!(
            history_path(Ok(" /data/history.sqlite ".to_owned())).unwrap(),
            PathBuf::from("/data/history.sqlite")
        );
    }
}
