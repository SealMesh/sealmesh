//! The one way a tool reaches the router, and so the one place this crate
//! touches a credential.
//!
//! There is no credential code here. [`RouterAccess::connect_reader`] hands
//! off to `sealmesh::client::connect`, which asks
//! `sealmesh::github_auth::token` for whatever `gh` hands over, moves it into
//! the `Hello` frame, and drops it once that frame is sent. Nothing in this
//! crate holds a `SecretToken`, so nothing here can store or log one (rules 6,
//! 7). A second path that obtains a token would be a second place to get that
//! wrong; a tool that needs the router calls this instead.

use std::path::PathBuf;

use anyhow::Result;
use sealmesh::{
    client::{self, Connected},
    config::Config,
};
use sealmesh_proto::Role;

/// What a tool needs to open a connection to the router: where the config
/// lives, and nothing else. It is loaded per call rather than at startup, so a
/// machine with no config can still mount the server and complete the
/// handshake, and an edited config takes effect without remounting.
///
/// Not `Default`: a default would read this user's real config, so a test
/// built from one could reach a real router with a real credential.
#[derive(Debug, Clone)]
pub struct RouterAccess {
    config: Option<PathBuf>,
}

impl RouterAccess {
    pub fn new(config: Option<PathBuf>) -> Self {
        Self { config }
    }

    /// Open a reader connection, authenticated by the CLI's credential path.
    ///
    /// The config is read first, so a missing or invalid one fails before the
    /// credential is ever requested.
    pub async fn connect_reader(&self) -> Result<Connected> {
        let config = self.load_config()?;
        Self::connect_reader_with(&config).await
    }

    /// The first half of [`Self::connect_reader`], on its own so a tool can
    /// tell "this machine has no usable config" apart from "the router could
    /// not be reached" without reading an error message.
    pub fn load_config(&self) -> Result<Config> {
        Config::load(self.config.as_deref())
    }

    /// The second half of [`Self::connect_reader`]: the CLI's credential path
    /// and the reader handshake, against a config already loaded.
    pub async fn connect_reader_with(config: &Config) -> Result<Connected> {
        client::connect(&config.server, Role::Reader).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[tokio::test]
    async fn a_missing_config_is_an_error_that_names_the_config() {
        // This pins the error an engineer sees, not the ordering. That
        // `Config::load` runs before `client::connect` is a property of the code
        // above; with `GH_TOKEN` set in the environment, a reversed order would
        // still fail here on the config, and observing the credential lookup
        // would need process-wide environment mutation.
        let access = RouterAccess::new(Some(PathBuf::from(
            "/nonexistent/sealmesh-mcp-test/config.toml",
        )));
        let Err(error) = access.connect_reader().await else {
            panic!("a config that does not exist must not produce a connection");
        };
        assert!(format!("{error:#}").contains("reading config"), "{error:#}");
    }

    #[test]
    fn router_access_holds_a_path_and_no_credential() {
        let access = RouterAccess::new(Some(PathBuf::from("/tmp/sealmesh.toml")));
        assert_eq!(
            format!("{access:?}"),
            r#"RouterAccess { config: Some("/tmp/sealmesh.toml") }"#
        );
    }
}
