use std::{collections::BTreeSet, path::Path};

use gix::bstr::BStr;

use crate::gitref;

use super::{RepoFacts, RepoResolver};

#[derive(Debug, Clone)]
pub struct GitResolver {
    integration_branch: String,
}

impl GitResolver {
    pub fn new(integration_branch: impl Into<String>) -> Self {
        Self {
            integration_branch: integration_branch.into(),
        }
    }
}

impl RepoResolver for GitResolver {
    fn resolve(&self, dir: &Path) -> Option<RepoFacts> {
        let repo = gix::discover(dir).ok()?;
        let slug = gitref::origin(&repo)?;
        Some(RepoFacts {
            branch: gitref::branch(&repo),
            ahead: ahead_of_integration(&repo, &self.integration_branch),
            dirty_files: dirty_file_count(&repo),
            slug,
        })
    }
}

fn ahead_of_integration(repo: &gix::Repository, integration: &str) -> Option<u32> {
    let head = repo.head_id().ok()?;
    let spec = format!("refs/remotes/origin/{integration}");
    let base = repo.rev_parse_single(BStr::new(spec.as_bytes())).ok()?;
    let walk = repo
        .rev_walk(Some(head.detach()))
        .with_hidden(Some(base.detach()))
        .all()
        .ok()?;
    u32::try_from(walk.count()).ok()
}

fn dirty_file_count(repo: &gix::Repository) -> Option<u32> {
    let iter = repo
        .status(gix::progress::Discard)
        .ok()?
        .untracked_files(gix::status::UntrackedFiles::Files)
        .into_iter(std::iter::empty())
        .ok()?;

    let mut paths: BTreeSet<Vec<u8>> = BTreeSet::new();
    for item in iter {
        let Ok(item) = item else { continue };
        let path = match item {
            gix::status::Item::IndexWorktree(change) => change.rela_path().to_vec(),
            gix::status::Item::TreeIndex(change) => change.location().to_vec(),
        };
        paths.insert(path);
    }
    u32::try_from(paths.len()).ok()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn the_dirty_count_actually_counts() {
        let dir = std::env::temp_dir().join("sealmesh-git-dirty");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let repo = gix::init(&dir).unwrap();

        assert_eq!(dirty_file_count(&repo), Some(0), "a fresh repo is clean");

        std::fs::write(dir.join("a.txt"), "one").unwrap();
        std::fs::write(dir.join("b.txt"), "two").unwrap();
        let repo = gix::open(&dir).unwrap();
        assert_eq!(dirty_file_count(&repo), Some(2));

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A checkout with a GitHub origin resolves to that origin's slug. A
    /// scratch repository rather than this source tree, whose origin is
    /// whatever remote it was cloned from (a fork, or none).
    #[test]
    fn a_github_checkout_reads_back_correctly() {
        let dir = std::env::temp_dir().join(format!("sealmesh-git-github-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let git = |args: &[&str]| {
            let ok = std::process::Command::new("git")
                .args(args)
                .current_dir(&dir)
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            assert!(ok, "git {args:?} failed");
        };
        git(&["init", "--initial-branch=main", "."]);
        git(&[
            "remote",
            "add",
            "origin",
            "https://github.com/example-org/sealmesh.git",
        ]);
        let facts = GitResolver::new("main").resolve(&dir).unwrap();
        assert_eq!(facts.slug.to_string(), "example-org/sealmesh");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `who` has always shown a checkout by its origin's last two path
    /// segments, whatever the host. Moving the parser into `gitref`, where
    /// rooms read only GitHub origins, must not narrow what `who` shows.
    #[test]
    fn who_still_shows_an_origin_on_any_host() {
        let dir = std::env::temp_dir().join(format!("sealmesh-git-anyhost-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let git = |args: &[&str]| {
            let ok = std::process::Command::new("git")
                .args(args)
                .current_dir(&dir)
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            assert!(ok, "git {args:?} failed");
        };
        git(&["init", "--initial-branch=feature/eng-9-x", "."]);
        git(&[
            "remote",
            "add",
            "origin",
            "https://gitlab.com/example-org/example-repo.git",
        ]);
        let facts = GitResolver::new("main").resolve(&dir).unwrap();
        assert_eq!(facts.slug.as_str(), "example-org/example-repo");
        assert_eq!(
            facts.branch.as_ref().map(|b| b.as_str()),
            Some("feature/eng-9-x")
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_detached_head_yields_no_branch() {
        let dir = std::env::temp_dir().join("sealmesh-git-detached");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();

        let git = |args: &[&str]| {
            let ok = std::process::Command::new("git")
                .args(args)
                .current_dir(&dir)
                .env("GIT_AUTHOR_NAME", "t")
                .env("GIT_AUTHOR_EMAIL", "t@example.test")
                .env("GIT_COMMITTER_NAME", "t")
                .env("GIT_COMMITTER_EMAIL", "t@example.test")
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            assert!(ok, "git {args:?} failed");
        };
        git(&["init", "--initial-branch=main", "."]);
        std::fs::write(dir.join("a.txt"), "one").unwrap();
        git(&["add", "."]);
        git(&["commit", "-m", "one"]);

        let repo = gix::open(&dir).unwrap();
        assert!(gitref::branch(&repo).is_some(), "on a branch");

        git(&["checkout", "--detach", "HEAD"]);
        let repo = gix::open(&dir).unwrap();
        assert_eq!(
            gitref::branch(&repo),
            None,
            "detached HEAD invented a branch"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
