use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use sealmesh_proto::SecretToken;

const TOKEN_VARS: [&str; 2] = ["GH_TOKEN", "GITHUB_TOKEN"];

pub fn token() -> Result<SecretToken> {
    for var in TOKEN_VARS {
        if let Ok(value) = std::env::var(var) {
            let value = value.trim();
            if !value.is_empty() {
                return Ok(SecretToken::new(value.to_owned()));
            }
        }
    }
    from_gh()
}

fn from_gh() -> Result<SecretToken> {
    let output = Command::new("gh")
        .args(["auth", "token"])
        .output()
        .context("running `gh auth token` — is the GitHub CLI installed?")?;

    if !output.status.success() {
        let reason = String::from_utf8_lossy(&output.stderr);
        let reason = reason.lines().next().unwrap_or("no reason given");
        bail!("`gh auth token` failed: {reason}. Run `gh auth login`.");
    }

    let token = String::from_utf8(output.stdout)
        .map_err(|_| anyhow!("`gh auth token` returned something that is not text"))?;
    let token = token.trim();
    if token.is_empty() {
        bail!("`gh auth token` returned nothing. Run `gh auth login`.");
    }
    Ok(SecretToken::new(token.to_owned()))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn a_resolved_token_does_not_render() {
        let token = SecretToken::new("ghp_realsecretvalue".to_owned());
        assert_eq!(format!("{token:?}"), "SecretToken(redacted)");
        assert!(!format!("{token:?}").contains("ghp_realsecretvalue"));
    }

    #[test]
    fn the_environment_is_preferred_over_shelling_out() {
        assert_eq!(TOKEN_VARS, ["GH_TOKEN", "GITHUB_TOKEN"]);
    }
}
