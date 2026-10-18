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

    #[cfg(feature = "dynamodb")]
    #[test]
    fn the_table_variable_is_the_one_the_dynamodb_store_reads() {
        assert_eq!(HISTORY_TABLE_VAR, crate::history::TABLE_VAR);
    }
}
