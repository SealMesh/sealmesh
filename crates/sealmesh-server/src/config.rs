//! The router's config file.
//!
//! Every setting can come from `sealmesh-server.toml` (`--config PATH`, or
//! [`CONFIG_VAR`]) or from its environment variable, and **the environment
//! wins**. A file value is handed to the same parser as the environment
//! variable it stands for, so a value is validated exactly once, in one place,
//! whichever way it arrived.
//!
//! ```toml
//! github_org = "example-org"            # SEALMESH_GITHUB_ORG
//! listen = "0.0.0.0:8787"               # SEALMESH_LISTEN
//! projects = [                          # SEALMESH_PROJECTS
//!   { id = "example", repos = ["example-org/example-repo"] },
//! ]
//! # Optional limits, each defaulted when absent:
//! # access_cache_ttl_secs, room_tokens_per_hour, room_turn_cap,
//! # reader_checks_per_hour, tls_terminated_by
//!
//! [history]
//! backend = "sqlite"                    # SEALMESH_HISTORY_BACKEND
//! path = "/data/history.sqlite"         # SEALMESH_HISTORY_PATH
//! # table = "..."                       # DYNAMODB_ROOM_HISTORY, backend = "dynamodb"
//! ```
//!
//! An otw router (on one of your machines, reached over Tailscale) sets
//! `mode` and an `[otw]` allowlist instead of `github_org`:
//!
//! ```toml
//! mode = "otw"                          # SEALMESH_MODE; "cloud" when absent
//! listen = "100.101.102.103:8787"       # this machine's tailnet address
//!
//! [otw]
//! allow_users = ["alice@github"]        # Tailscale login names
//! allow_tags = ["tag:sealmesh"]         # ACL tags
//! # tailscaled_socket = "/var/run/tailscale/tailscaled.sock"
//!
//! [history]
//! path = "/var/lib/sealmesh/history.sqlite"
//!
//! [[room]]                              # optional named rooms, otw only
//! name = "ops"
//! members = ["alice-github", "tag-sealmesh"]
//! ```

use std::{
    env::VarError,
    path::{Path, PathBuf},
};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

use crate::{
    check_budget::CHECKS_PER_HOUR_VAR, listen::TLS_TERMINATOR_VAR, projects::PROJECTS_VAR,
    room_budget::TOKENS_PER_HOUR_VAR, turn_cap::TURN_CAP_VAR,
};

/// Where the config file is, when `--config` isn't given.
pub const CONFIG_VAR: &str = "SEALMESH_CONFIG";
pub const ORG_VAR: &str = "SEALMESH_GITHUB_ORG";
/// `cloud` (the default) or `otw`.
pub const MODE_VAR: &str = "SEALMESH_MODE";
pub const LISTEN_VAR: &str = "SEALMESH_LISTEN";
pub const ACCESS_CACHE_TTL_VAR: &str = "SEALMESH_ACCESS_CACHE_TTL_SECS";
/// `sqlite` (the default) or `dynamodb` (a build with the `dynamodb` feature).
pub const HISTORY_BACKEND_VAR: &str = "SEALMESH_HISTORY_BACKEND";
/// The SQLite file. Required for the `sqlite` backend, with no default: a
/// guessed path writes an org's coordination history somewhere nobody looks.
pub const HISTORY_PATH_VAR: &str = "SEALMESH_HISTORY_PATH";
/// The DynamoDB table, for the `dynamodb` backend.
pub const HISTORY_TABLE_VAR: &str = "DYNAMODB_ROOM_HISTORY";

/// `sealmesh-server.toml`. Every field is optional here; whether a setting is
/// required is decided by its parser, the same as for its environment variable.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub mode: Option<String>,
    pub github_org: Option<String>,
    pub listen: Option<String>,
    pub projects: Option<Vec<ProjectConfig>>,
    pub access_cache_ttl_secs: Option<u64>,
    pub room_tokens_per_hour: Option<u64>,
    pub room_turn_cap: Option<u64>,
    pub reader_checks_per_hour: Option<u64>,
    pub tls_terminated_by: Option<String>,
    #[serde(default)]
    pub history: HistoryConfig,
    /// `[otw]`: who an otw router admits. Read only in otw mode.
    pub otw: Option<OtwConfig>,
    /// Named rooms (`[[room]]`), otw only: a cloud router decides rooms from
    /// GitHub and refuses to start with one.
    #[serde(default, rename = "room")]
    pub rooms: Vec<RoomConfig>,
}

/// One `[[room]]`: a named room and the hosts that may join it. A host here
/// is what the handshake folds an identity into (`alice@github` is
/// `alice-github`, `tag:sealmesh` is `tag-sealmesh`).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoomConfig {
    pub name: sealmesh_chat_wire::RoomName,
    pub members: Vec<sealmesh_wire::HostId>,
}

impl ServerConfig {
    /// The `[[room]]` blocks as the router's named-room table. A duplicate
    /// name or an empty member list is refused, as the daemon refuses them.
    pub fn named_rooms(&self) -> anyhow::Result<crate::named_rooms::NamedRooms> {
        let mut table = crate::named_rooms::NamedRooms::new();
        for room in &self.rooms {
            if table.members_of(&room.name).is_some() {
                anyhow::bail!("[[room]] {} is declared twice", room.name);
            }
            if room.members.is_empty() {
                anyhow::bail!(
                    "[[room]] {} lists no members, so nobody could join it",
                    room.name
                );
            }
            table.insert(room.name.clone(), room.members.iter().cloned());
        }
        Ok(table)
    }
}

/// Which router this is: a GitHub-backed cloud router, or an otw router whose
/// identity comes from Tailscale.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ServerMode {
    #[default]
    Cloud,
    Otw,
}

impl ServerMode {
    /// Parses [`MODE_VAR`]'s value. Absent is `cloud`, which is what every
    /// deployment from before modes means; anything unrecognised is an error,
    /// not a guess.
    pub fn parse(value: Result<String, VarError>) -> anyhow::Result<Self> {
        match value {
            Err(VarError::NotPresent) => Ok(Self::Cloud),
            Err(VarError::NotUnicode(_)) => anyhow::bail!("{MODE_VAR} is not valid UTF-8"),
            Ok(raw) => match raw.trim() {
                "cloud" => Ok(Self::Cloud),
                "otw" => Ok(Self::Otw),
                "std" => anyhow::bail!(
                    "{MODE_VAR} is \"std\", but a std router runs inside `sealmesh daemon`, \
                     not as sealmesh-server. Use \"cloud\" or \"otw\""
                ),
                other => anyhow::bail!("{MODE_VAR} must be \"cloud\" or \"otw\", not {other:?}"),
            },
        }
    }
}

/// `[otw]`: the router's own allowlist, on top of the tailnet ACL. Both lists
/// empty admits nobody, so the router refuses to start.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtwConfig {
    #[serde(default)]
    pub allow_users: Vec<String>,
    #[serde(default)]
    pub allow_tags: Vec<String>,
    /// The `tailscaled` LocalAPI socket. Defaults to the open-source daemon's
    /// path on this platform (`crate::tailscale::default_socket`).
    pub tailscaled_socket: Option<PathBuf>,
}

/// One project: an id and the repos it spans.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
    pub id: String,
    pub repos: Vec<String>,
}

/// `[history]`: where room messages are kept.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryConfig {
    pub backend: Option<String>,
    pub path: Option<PathBuf>,
    pub table: Option<String>,
}

impl ServerConfig {
    /// Reads and parses the file at `path`. An unknown key is an error, so a
    /// typo is a startup failure rather than a setting silently ignored.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading the config file {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parsing the config file {}", path.display()))
    }

    /// The value for `var`: the environment's if it is set, otherwise the
    /// file's, otherwise `Err(VarError::NotPresent)` — exactly what
    /// `std::env::var` returns, so every existing parser takes it unchanged.
    pub fn setting(&self, var: &str) -> Result<String, VarError> {
        self.setting_with(var, |name| std::env::var(name))
    }

    /// [`Self::setting`] with the environment passed in, so a test never
    /// touches the real process environment.
    pub fn setting_with(
        &self,
        var: &str,
        env: impl Fn(&str) -> Result<String, VarError>,
    ) -> Result<String, VarError> {
        match env(var) {
            Err(VarError::NotPresent) => self.file_value(var).ok_or(VarError::NotPresent),
            other => other,
        }
    }

    fn file_value(&self, var: &str) -> Option<String> {
        let number = |value: Option<u64>| value.map(|n| n.to_string());
        match var {
            MODE_VAR => self.mode.clone(),
            ORG_VAR => self.github_org.clone(),
            LISTEN_VAR => self.listen.clone(),
            // The project map's own parser reads JSON; the file's table is
            // re-encoded to that, so it is validated by the same code.
            PROJECTS_VAR => self
                .projects
                .as_ref()
                .and_then(|projects| serde_json::to_string(projects).ok()),
            ACCESS_CACHE_TTL_VAR => number(self.access_cache_ttl_secs),
            TOKENS_PER_HOUR_VAR => number(self.room_tokens_per_hour),
            TURN_CAP_VAR => number(self.room_turn_cap),
            CHECKS_PER_HOUR_VAR => number(self.reader_checks_per_hour),
            TLS_TERMINATOR_VAR => self.tls_terminated_by.clone(),
            HISTORY_BACKEND_VAR => self.history.backend.clone(),
            HISTORY_PATH_VAR => self
                .history
                .path
                .as_ref()
                .map(|path| path.display().to_string()),
            HISTORY_TABLE_VAR => self.history.table.clone(),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::projects::ProjectMap;

    fn parse(text: &str) -> ServerConfig {
        toml::from_str(text).unwrap()
    }

    fn unset(_: &str) -> Result<String, VarError> {
        Err(VarError::NotPresent)
    }

    const FILE: &str = r#"
        github_org = "example-org"
        listen = "127.0.0.1:9000"
        projects = [{ id = "example", repos = ["example-org/example-repo", "example-org/other"] }]
        room_turn_cap = 12

        [history]
        backend = "sqlite"
        path = "/data/history.sqlite"
    "#;

    #[test]
    fn a_file_value_is_used_when_the_environment_has_none() {
        let config = parse(FILE);
        assert_eq!(config.setting_with(ORG_VAR, unset).unwrap(), "example-org");
        assert_eq!(
            config.setting_with(LISTEN_VAR, unset).unwrap(),
            "127.0.0.1:9000"
        );
        assert_eq!(config.setting_with(TURN_CAP_VAR, unset).unwrap(), "12");
        assert_eq!(
            config.setting_with(HISTORY_BACKEND_VAR, unset).unwrap(),
            "sqlite"
        );
        assert_eq!(
            config.setting_with(HISTORY_PATH_VAR, unset).unwrap(),
            "/data/history.sqlite"
        );
    }

    #[test]
    fn the_environment_wins_over_the_file() {
        let config = parse(FILE);
        let env = |name: &str| {
            if name == ORG_VAR {
                Ok("from-env".to_owned())
            } else {
                Err(VarError::NotPresent)
            }
        };
        assert_eq!(config.setting_with(ORG_VAR, env).unwrap(), "from-env");
        assert_eq!(
            config.setting_with(LISTEN_VAR, env).unwrap(),
            "127.0.0.1:9000"
        );
    }

    #[test]
    fn an_absent_setting_reads_as_not_present_so_its_parser_decides() {
        let config = ServerConfig::default();
        assert_eq!(
            config.setting_with(ORG_VAR, unset),
            Err(VarError::NotPresent)
        );
        assert_eq!(
            config.setting_with(TOKENS_PER_HOUR_VAR, unset),
            Err(VarError::NotPresent)
        );
    }

    #[test]
    fn the_files_projects_go_through_the_project_maps_own_parser() {
        let config = parse(FILE);
        let map = ProjectMap::parse(&config.setting_with(PROJECTS_VAR, unset).unwrap()).unwrap();
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn an_unknown_key_is_refused_rather_than_ignored() {
        assert!(toml::from_str::<ServerConfig>("github_orgg = \"typo\"").is_err());
        assert!(toml::from_str::<ServerConfig>("[history]\nbakend = \"sqlite\"").is_err());
    }

    #[test]
    fn the_mode_defaults_to_cloud_and_reads_otw() {
        assert_eq!(
            ServerMode::parse(ServerConfig::default().setting_with(MODE_VAR, unset)).unwrap(),
            ServerMode::Cloud
        );
        let otw = parse(
            r#"
            mode = "otw"
            listen = "100.101.102.103:8787"
            [otw]
            allow_users = ["alice@github"]
            allow_tags = ["tag:sealmesh"]
            tailscaled_socket = "/tmp/ts.sock"
        "#,
        );
        assert_eq!(
            ServerMode::parse(otw.setting_with(MODE_VAR, unset)).unwrap(),
            ServerMode::Otw
        );
        let section = otw.otw.unwrap();
        assert_eq!(section.allow_users, ["alice@github"]);
        assert_eq!(section.allow_tags, ["tag:sealmesh"]);
        assert_eq!(
            section.tailscaled_socket,
            Some(PathBuf::from("/tmp/ts.sock"))
        );
    }

    #[test]
    fn named_rooms_are_read_and_checked() {
        let config = parse(
            r#"
            [[room]]
            name = "ops"
            members = ["alice-github", "tag-sealmesh"]
        "#,
        );
        let table = config.named_rooms().unwrap();
        let ops = sealmesh_chat_wire::RoomName::try_from("ops").unwrap();
        let alice = sealmesh_wire::HostId::try_from("alice-github").unwrap();
        assert!(table.admits(&ops, &alice));
        for bad in [
            "[[room]]\nname = \"ops\"\nmembers = []",
            "[[room]]\nname = \"ops\"\nmembers = [\"a\"]\n[[room]]\nname = \"ops\"\nmembers = [\"b\"]",
        ] {
            assert!(parse(bad).named_rooms().is_err(), "{bad}");
        }
        assert!(ServerConfig::default().named_rooms().unwrap().is_empty());
    }

    #[test]
    fn an_unknown_mode_or_otw_key_is_refused() {
        assert!(ServerMode::parse(Ok("std".to_owned())).is_err());
        assert!(ServerMode::parse(Ok("tailscale".to_owned())).is_err());
        assert!(toml::from_str::<ServerConfig>("[otw]\nallow_user = [\"a\"]").is_err());
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn the_table_variable_is_the_one_the_dynamodb_store_reads() {
        assert_eq!(HISTORY_TABLE_VAR, crate::history::TABLE_VAR);
    }
}
