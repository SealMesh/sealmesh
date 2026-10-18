use std::{collections::BTreeMap, process::Command};

use jiff::Timestamp;
use octocrab::{Octocrab, params::repos::Commitish};
use serde::{Deserialize, Serialize};

use crate::{
    progress::{Phase, Progress},
    wire::RepoSlug,
};

use super::cache::Cache;

const TTL_SECS: i64 = 60;

const FAILURE_TTL_SECS: i64 = 15;

const MAX_LOOKUPS: usize = 32;

const RESOLVE_BUDGET: std::time::Duration = std::time::Duration::from_secs(8);

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrStatus {
    pub state: PrState,
    pub checks: CheckState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrState {
    Draft,
    Open,
    Merged,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckState {
    Green,
    Red,
    Running,
    None,
    Unknown,
}

impl PrStatus {
    pub const fn unknown() -> Self {
        Self {
            state: PrState::Open,
            checks: CheckState::Unknown,
        }
    }
}

pub async fn resolve(
    wanted: &[(RepoSlug, u32)],
    now: Timestamp,
    progress: &Progress,
) -> BTreeMap<String, PrStatus> {
    let mut cache: Cache<PrStatus> = Cache::load("github", TTL_SECS);
    let mut out = BTreeMap::new();

    let mut misses: Vec<&(RepoSlug, u32)> = Vec::new();
    for pair in wanted {
        let key = cache_key(&pair.0, pair.1);
        match cache.get(&key, now) {
            Some(status) => {
                out.insert(key, status);
            }
            None => misses.push(pair),
        }
    }
    if misses.is_empty() {
        return out;
    }

    let Some(client) = client() else {
        return out;
    };

    let dropped = misses.len().saturating_sub(MAX_LOOKUPS);
    if dropped > 0 {
        progress.note(&format!(
            "{dropped} pull requests not looked up this run (per-run budget)"
        ));
    }

    let planned = misses.len().min(MAX_LOOKUPS);
    progress.set(Phase::Resolving {
        done: 0,
        total: planned,
    });

    let lookups = async {
        let mut done = 0usize;
        for (repo, number) in misses.into_iter().take(MAX_LOOKUPS) {
            let key = cache_key(repo, *number);
            match fetch(&client, repo, *number).await {
                Some(status) => {
                    cache.put(key.clone(), status, now);
                    out.insert(key, status);
                }
                None => cache.put_for(key, PrStatus::unknown(), now, FAILURE_TTL_SECS),
            }
            done = done.saturating_add(1);
            progress.set(Phase::Resolving {
                done,
                total: planned,
            });
        }
    };

    if tokio::time::timeout(RESOLVE_BUDGET, lookups).await.is_err() {
        progress.note("pull request lookups timed out; showing what resolved");
    }
    cache.save(now);
    out
}

pub fn cache_key(repo: &RepoSlug, number: u32) -> String {
    format!("{repo}#{number}")
}

fn client() -> Option<Octocrab> {
    let token = gh_token()?;
    Octocrab::builder()
        .personal_token(token)
        .set_connect_timeout(Some(CONNECT_TIMEOUT))
        .set_read_timeout(Some(READ_TIMEOUT))
        .set_write_timeout(Some(READ_TIMEOUT))
        .build()
        .ok()
}

fn gh_token() -> Option<String> {
    let output = Command::new("gh").args(["auth", "token"]).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let token = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!token.is_empty()).then_some(token)
}

async fn fetch(client: &Octocrab, repo: &RepoSlug, number: u32) -> Option<PrStatus> {
    let pr = client
        .pulls(repo.owner(), repo.repo())
        .get(u64::from(number))
        .await
        .ok()?;

    let state = if pr.merged_at.is_some() {
        PrState::Merged
    } else if pr.state == Some(octocrab::models::IssueState::Closed) {
        PrState::Closed
    } else if pr.draft == Some(true) {
        PrState::Draft
    } else {
        PrState::Open
    };

    if matches!(state, PrState::Merged | PrState::Closed) {
        return Some(PrStatus {
            state,
            checks: CheckState::None,
        });
    }

    let checks = match pr.head.sha.as_str() {
        "" => CheckState::Unknown,
        sha => check_state(client, repo, sha).await,
    };
    Some(PrStatus { state, checks })
}

async fn check_state(client: &Octocrab, repo: &RepoSlug, sha: &str) -> CheckState {
    let Ok(runs) = client
        .checks(repo.owner(), repo.repo())
        .list_check_runs_for_git_ref(Commitish(sha.to_owned()))
        .per_page(100u8)
        .send()
        .await
    else {
        return CheckState::Unknown;
    };

    if runs.check_runs.is_empty() {
        return CheckState::None;
    }
    let mut running = false;
    for run in &runs.check_runs {
        match run.conclusion.as_deref() {
            Some("success" | "neutral" | "skipped") => {}
            Some("failure" | "timed_out" | "cancelled" | "action_required" | "stale") => {
                return CheckState::Red;
            }
            _ => running = true,
        }
    }
    if running {
        CheckState::Running
    } else {
        CheckState::Green
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn the_cache_key_names_a_pull_request_uniquely_across_repos() {
        let example_repo = RepoSlug::try_from("example-org/example-repo").unwrap();
        let watch = RepoSlug::try_from("example-org/sealmesh").unwrap();
        assert_eq!(
            cache_key(&example_repo, 627),
            "example-org/example-repo#627"
        );
        assert_ne!(cache_key(&example_repo, 627), cache_key(&watch, 627));
    }

    #[test]
    fn an_unreachable_api_is_never_optimistic() {
        assert_eq!(PrStatus::unknown().checks, CheckState::Unknown);
        assert_ne!(PrStatus::unknown().checks, CheckState::Green);
    }

    #[tokio::test]
    async fn a_real_pull_request_resolves_when_gh_is_authenticated() {
        let Some(client) = client() else {
            eprintln!("skipped: no gh token");
            return;
        };
        let repo = RepoSlug::try_from("example-org/sealmesh").unwrap();
        let Some(status) = fetch(&client, &repo, 3).await else {
            eprintln!("skipped: PR #3 not reachable");
            return;
        };
        assert_ne!(status.checks, CheckState::Unknown, "{status:?}");
    }
}
