use std::path::PathBuf;

pub fn config_home() -> Option<PathBuf> {
    xdg("XDG_CONFIG_HOME", ".config")
}

pub fn cache_home() -> Option<PathBuf> {
    xdg("XDG_CACHE_HOME", ".cache")
}

pub fn state_home() -> Option<PathBuf> {
    xdg("XDG_STATE_HOME", ".local/state")
}

pub fn declared_agents_dir() -> Option<PathBuf> {
    state_home().map(|d| d.join("sealmesh/agents"))
}

/// The directory holding the daemon's local room socket, and nothing else, so
/// it can be 0700 without touching the rest of the state directory.
pub fn local_socket_dir() -> Option<PathBuf> {
    state_home().map(|d| d.join("sealmesh/local"))
}

/// The directory the std-mode embedded router keeps its per-boot secret in,
/// with its socket and history. 0700, owned by the user running it.
pub fn local_router_dir() -> Option<PathBuf> {
    state_home().map(|d| d.join("sealmesh/router"))
}

/// The std-mode embedded router's per-boot secret: written 0600 by the
/// router when it starts, read by `CredentialSource::LocalSecret`.
pub fn local_router_secret() -> Option<PathBuf> {
    local_router_dir().map(|d| d.join("secret"))
}

/// The std-mode embedded router's Unix socket (0600, in the 0700 router
/// directory). The router has no TCP listener in std mode; this is its only
/// way in.
pub fn local_router_socket() -> Option<PathBuf> {
    local_router_dir().map(|d| d.join("router.sock"))
}

/// The std-mode embedded router's room history (SQLite, 7-day retention).
pub fn local_router_history() -> Option<PathBuf> {
    local_router_dir().map(|d| d.join("history.sqlite"))
}

/// `agents.sock`: the std daemon's HTTP-over-Unix-socket link for configured
/// agents (`[[agent]]`). 0600, in `$XDG_STATE_HOME/sealmesh`, which is made
/// 0700.
pub fn agents_socket() -> Option<PathBuf> {
    state_home().map(|d| d.join("sealmesh/agents.sock"))
}

/// Where each configured agent's bearer token lives, as `<name>.token`
/// (0600). The same directory `sealmesh declare` writes `<id>.json` records
/// to; each reader looks only at its own extension.
pub fn agent_tokens_dir() -> Option<PathBuf> {
    declared_agents_dir()
}

fn xdg(var: &str, fallback: &str) -> Option<PathBuf> {
    resolve(std::env::var_os(var), fallback, dirs::home_dir())
}

fn resolve(
    var: Option<std::ffi::OsString>,
    fallback: &str,
    home: Option<PathBuf>,
) -> Option<PathBuf> {
    if let Some(dir) = var {
        let path = PathBuf::from(dir);
        if path.is_absolute() {
            return Some(path);
        }
    }
    home.map(|home| home.join(fallback))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn os(s: &str) -> Option<std::ffi::OsString> {
        Some(std::ffi::OsString::from(s))
    }

    fn home() -> Option<PathBuf> {
        Some(PathBuf::from("/Users/sandbank"))
    }

    #[test]
    fn an_unset_or_unusable_xdg_var_falls_back_to_dot_config_not_macos_native() {
        for var in [None, os(""), os("relative/path"), os(".")] {
            let path = resolve(var.clone(), ".config", home()).unwrap();
            assert_eq!(
                path,
                PathBuf::from("/Users/sandbank/.config"),
                "{var:?} resolved to {}",
                path.display()
            );
            assert!(
                !path.to_string_lossy().contains("Library"),
                "resolved to a macOS-native path: {}",
                path.display()
            );
        }
    }

    #[test]
    fn an_absolute_xdg_var_wins() {
        assert_eq!(
            resolve(os("/tmp/xdg"), ".config", home()).unwrap(),
            PathBuf::from("/tmp/xdg")
        );
    }

    #[test]
    fn no_home_and_no_usable_var_resolves_to_nothing() {
        assert!(resolve(None, ".config", None).is_none());
        assert!(resolve(os("relative"), ".cache", None).is_none());
    }

    #[test]
    fn the_two_homes_are_wired_to_the_right_variable_and_fallback() {
        for (got, var, fallback) in [
            (config_home(), "XDG_CONFIG_HOME", ".config"),
            (cache_home(), "XDG_CACHE_HOME", ".cache"),
        ] {
            let want = resolve(std::env::var_os(var), fallback, dirs::home_dir());
            assert_eq!(got, want, "{var} is not wired to {fallback}");
        }
    }
}
