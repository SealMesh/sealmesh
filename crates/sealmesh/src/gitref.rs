//! What a checkout is: its origin's `owner/repo`, its current branch, and the
//! ticket that branch names. Nothing else.
//!
//! One parser for all of it, shared by the collector and by anything that
//! needs to know which rooms a checkout belongs to — the hook, the MCP room
//! tools, `sealmesh chat`. It is its own module rather than part of
//! `collect` because the MCP crate may not call `collect`, and because this is
//! the cheap half of what `collect` learns about a repo: it opens the
//! repository and reads its config and `HEAD`, and never walks the working
//! tree, the index or the history. `collect`'s dirty count and ahead count do,
//! and stay there.
//!
//! What it costs: `gix::discover` walks up from the directory to the first
//! `.git`, then reads the repository's config files and `HEAD` — a handful of
//! small file reads, no object and no index. Measured on this repository on a
//! laptop, in a release build: about 0.3 ms a call warm and 3 ms for the first
//! call in a process. It is flat in the size of the repository, so a hook may
//! call it on every prompt.
//!
//! Every value comes back through its validating newtype — [`RepoSlug`],
//! [`BranchName`], [`TicketId`] — so nothing here hands out a string a remote
//! URL or a branch name chose (rules 9, 45). Anything unexpected is `None`,
//! never a panic: no repository, a bare repository, no `origin`, an `origin`
//! that is not on GitHub, a detached `HEAD`, a branch naming no ticket.

use std::path::Path;

use gix::bstr::ByteSlice as _;

use crate::wire::{BranchName, RepoSlug, TicketId};

/// A checkout, as rooms see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkout {
    /// The `origin` remote's `owner/repo`, on GitHub.
    pub repo: RepoSlug,
    /// `None` on a detached `HEAD`.
    pub branch: Option<BranchName>,
    /// The ticket the branch names, for one of `prefixes`.
    pub ticket: Option<TicketId>,
}

/// Reads the checkout `dir` is in. `None` when `dir` is in no repository, the
/// repository is bare, or its `origin` is missing or not on GitHub.
///
/// `prefixes` are the ticket prefixes to recognise, as `Config.ticket_prefixes`
/// lists them.
pub fn read(dir: &Path, prefixes: &[String]) -> Option<Checkout> {
    let repo = gix::discover(dir).ok()?;
    if repo.is_bare() {
        return None;
    }
    let slug = github_origin(&repo)?;
    let branch = branch(&repo);
    let ticket = branch
        .as_ref()
        .and_then(|b| ticket_from_branch(b, prefixes));
    Some(Checkout {
        repo: slug,
        branch,
        ticket,
    })
}

/// The `origin` remote's `owner/repo`, whatever host it names.
///
/// This is what `collect` has always published, and it keeps doing so: `who`
/// shows a checkout whose origin is on another forge, or reached through an
/// SSH alias, as its `owner/repo`. Rooms use [`github_origin`] instead.
pub(crate) fn origin(repo: &gix::Repository) -> Option<RepoSlug> {
    let remote = repo.find_remote("origin").ok()?;
    let url = remote.url(gix::remote::Direction::Fetch)?;
    let path = url.path.to_str().ok()?;
    slug_from_remote_path(path)
}

/// The `origin` remote's `owner/repo`, only when that remote is on GitHub.
///
/// A room is keyed by a GitHub repo, so a checkout of `gitlab.com/a/b` must
/// not land in the room of `github.com/a/b`. The router would refuse the join
/// unless the caller can read the GitHub repo, so this is about not naming the
/// wrong room rather than about access.
///
/// An SSH alias is the one host that cannot be read from the URL: `git@work:a/b`
/// names a `Host work` in `~/.ssh/config`. A host with no dot in it is taken to
/// be one, as is `github.com-<anything>`, the other common spelling.
///
/// So some hosts that are not GitHub read as GitHub: `git@gitlab-work:a/b`, or
/// a domain like `github.com-evil.com`. That is harmless, because naming a
/// room is all this can lead to. A room is named only when `a/b` is in the
/// project map this daemon holds, which the router already cut to GitHub repos
/// this machine's own token can read, and every join is still authorised by
/// the router against GitHub. The worst case is naming the room of a GitHub
/// repo the caller can already read.
fn github_origin(repo: &gix::Repository) -> Option<RepoSlug> {
    let remote = repo.find_remote("origin").ok()?;
    let url = remote.url(gix::remote::Direction::Fetch)?;
    match url.scheme {
        gix::url::Scheme::Ssh | gix::url::Scheme::Https | gix::url::Scheme::Git => {}
        gix::url::Scheme::File | gix::url::Scheme::Http | gix::url::Scheme::Ext(_) => {
            return None;
        }
    }
    if !is_github_host(url.host()?) {
        return None;
    }
    slug_from_remote_path(url.path.to_str().ok()?)
}

fn is_github_host(host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    host == "github.com"
        || host.ends_with(".github.com")
        || host.starts_with("github.com-")
        || (!host.contains('.') && !host.is_empty() && host != "localhost")
}

/// The last two segments of a remote's path, as `owner/repo`.
pub(crate) fn slug_from_remote_path(path: &str) -> Option<RepoSlug> {
    let trimmed = path.trim_matches('/');
    let trimmed = trimmed.strip_suffix(".git").unwrap_or(trimmed);
    let mut segments = trimmed.rsplit('/');
    let repo = segments.next()?;
    let owner = segments.next()?;
    RepoSlug::try_from(format!("{owner}/{repo}").as_str()).ok()
}

/// The branch `HEAD` is on. `None` when `HEAD` is detached.
pub(crate) fn branch(repo: &gix::Repository) -> Option<BranchName> {
    let head = repo.head_name().ok()??;
    BranchName::try_from(head.shorten().to_str().ok()?).ok()
}

/// The ticket a branch names, for one of `prefixes`: `feature/eng-557-x` is
/// `ENG-557` when `ENG` is a prefix. The first token that parses wins.
pub fn ticket_from_branch(branch: &BranchName, prefixes: &[String]) -> Option<TicketId> {
    branch
        .as_str()
        .split(|c: char| !c.is_ascii_alphanumeric() && c != '-')
        .find_map(|token| ticket_from_token(token, prefixes))
}

fn ticket_from_token(token: &str, prefixes: &[String]) -> Option<TicketId> {
    let (prefix, rest) = token.split_once('-')?;
    let prefix = prefix.to_ascii_uppercase();
    if !prefixes.iter().any(|p| p.eq_ignore_ascii_case(&prefix)) {
        return None;
    }
    let number = rest.split('-').next()?.parse::<u32>().ok()?;
    TicketId::new(&prefix, number).ok()
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use std::path::PathBuf;

    use super::*;

    fn branch_of(s: &str) -> BranchName {
        BranchName::try_from(s).unwrap()
    }

    fn eng() -> Vec<String> {
        vec!["ENG".to_owned()]
    }

    /// A scratch repository, removed on drop.
    struct Scratch(PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    impl Scratch {
        fn new(tag: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("sealmesh-gitref-{tag}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn git(&self, args: &[&str]) {
            let out = std::process::Command::new("git")
                .args(args)
                .current_dir(&self.0)
                .env("GIT_AUTHOR_NAME", "t")
                .env("GIT_AUTHOR_EMAIL", "t@example.test")
                .env("GIT_COMMITTER_NAME", "t")
                .env("GIT_COMMITTER_EMAIL", "t@example.test")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .output()
                .unwrap();
            assert!(out.status.success(), "git {args:?} failed: {out:?}");
        }

        /// A repository with one commit, on `branch`, with `origin` at `url`.
        fn checkout(tag: &str, branch: &str, url: Option<&str>) -> Self {
            let scratch = Self::new(tag);
            scratch.git(&["init", &format!("--initial-branch={branch}"), "."]);
            std::fs::write(scratch.0.join("a.txt"), "one").unwrap();
            scratch.git(&["add", "."]);
            scratch.git(&["commit", "-m", "one"]);
            if let Some(url) = url {
                scratch.git(&["remote", "add", "origin", url]);
            }
            scratch
        }
    }

    #[test]
    fn a_github_checkout_reads_its_repo_branch_and_ticket() {
        for url in [
            "git@github.com:example-org/example-repo.git",
            "https://github.com/example-org/example-repo.git",
            "https://github.com/example-org/example-repo",
            "ssh://git@ssh.github.com:443/example-org/example-repo.git",
            "git@github-work:example-org/example-repo.git",
            "git@github.com-work:example-org/example-repo.git",
        ] {
            let scratch = Scratch::checkout("gh", "feature/eng-557-rescore", Some(url));
            let got = read(&scratch.0, &eng()).unwrap_or_else(|| panic!("{url}"));
            assert_eq!(got.repo.as_str(), "example-org/example-repo", "{url}");
            assert_eq!(
                got.branch.as_ref().map(BranchName::as_str),
                Some("feature/eng-557-rescore")
            );
            assert_eq!(got.ticket.map(|t| t.to_string()), Some("ENG-557".into()));
        }
    }

    #[test]
    fn a_subdirectory_reads_as_its_checkout() {
        let scratch = Scratch::checkout(
            "sub",
            "main",
            Some("git@github.com:example-org/example-repo.git"),
        );
        let deep = scratch.0.join("a").join("b");
        std::fs::create_dir_all(&deep).unwrap();
        let got = read(&deep, &eng()).unwrap();
        assert_eq!(got.repo.as_str(), "example-org/example-repo");
        assert_eq!(got.ticket, None, "main names no ticket");
    }

    #[test]
    fn a_detached_head_keeps_the_repo_and_has_no_branch_or_ticket() {
        let scratch = Scratch::checkout(
            "det",
            "feature/eng-1-x",
            Some("git@github.com:example-org/example-repo.git"),
        );
        scratch.git(&["checkout", "--detach", "HEAD"]);
        let got = read(&scratch.0, &eng()).unwrap();
        assert_eq!(got.repo.as_str(), "example-org/example-repo");
        assert_eq!(got.branch, None, "a detached HEAD invented a branch");
        assert_eq!(got.ticket, None);
    }

    #[test]
    fn no_origin_is_none() {
        let scratch = Scratch::checkout("none", "main", None);
        assert_eq!(read(&scratch.0, &eng()), None);
        let scratch = Scratch::checkout("up", "main", None);
        scratch.git(&[
            "remote",
            "add",
            "upstream",
            "git@github.com:example-org/example-repo.git",
        ]);
        assert_eq!(read(&scratch.0, &eng()), None, "only origin counts");
    }

    #[test]
    fn an_origin_off_github_is_none_for_rooms_and_unchanged_for_collect() {
        for url in [
            "https://gitlab.com/example-org/example-repo.git",
            "git@bitbucket.org:example-org/example-repo.git",
            "http://github.com/example-org/example-repo.git",
            "file:///srv/git/example-org/example-repo.git",
            "/srv/git/example-org/example-repo.git",
            "ssh://git@localhost/example-org/example-repo.git",
        ] {
            let scratch = Scratch::checkout("off", "main", Some(url));
            assert_eq!(read(&scratch.0, &eng()), None, "{url}");
            // `who` has always shown these as their `owner/repo`, and still
            // does: the collector's reading is not narrowed.
            let repo = gix::discover(&scratch.0).unwrap();
            assert_eq!(
                origin(&repo).map(|s| s.to_string()),
                Some("example-org/example-repo".to_owned()),
                "{url}"
            );
        }
    }

    #[test]
    fn a_bare_repository_is_none() {
        let scratch = Scratch::new("bare");
        scratch.git(&["init", "--bare", "."]);
        scratch.git(&[
            "remote",
            "add",
            "origin",
            "git@github.com:example-org/example-repo.git",
        ]);
        assert_eq!(read(&scratch.0, &eng()), None);
    }

    #[test]
    fn a_directory_in_no_repository_is_none() {
        assert_eq!(read(Path::new("/"), &eng()), None);
    }

    #[test]
    fn a_hostile_branch_or_remote_path_is_none_rather_than_a_panic() {
        for path in ["", "/", "example-repo.git", "//", "a/..", "owner/re po"] {
            assert_eq!(slug_from_remote_path(path), None, "{path:?}");
        }
    }

    #[test]
    fn a_remote_behind_an_ssh_alias_still_resolves_to_its_slug() {
        for path in [
            "example-org/example-repo.git",
            "/example-org/example-repo.git",
            "example-org/example-repo",
            "/v3/example-org/example-repo.git",
        ] {
            assert_eq!(
                slug_from_remote_path(path).map(|s| s.to_string()),
                Some("example-org/example-repo".to_owned()),
                "{path}"
            );
        }
    }

    #[test]
    fn the_github_host_rule() {
        for host in [
            "github.com",
            "GitHub.com",
            "ssh.github.com",
            "github-work",
            "gh",
            "github.com-personal",
        ] {
            assert!(is_github_host(host), "{host}");
        }
        for host in ["gitlab.com", "bitbucket.org", "github.co", "localhost", ""] {
            assert!(!is_github_host(host), "{host}");
        }
    }

    /// A real checkout with a commit and a GitHub origin reads back as that
    /// origin's slug. A scratch repository rather than this source tree, whose
    /// origin is whatever remote it was cloned from (a fork, or none).
    #[test]
    fn a_github_checkout_reads_back_correctly() {
        let scratch = Scratch::checkout(
            "reads-back",
            "main",
            Some("https://github.com/example-org/sealmesh.git"),
        );
        let got = read(&scratch.0, &[]).unwrap();
        assert_eq!(got.repo.as_str(), "example-org/sealmesh");
    }

    #[test]
    fn a_lowercase_branch_slug_yields_an_uppercase_ticket() {
        for (name, want) in [
            ("feature/eng-557-qa-rescore-ui", "ENG-557"),
            ("chore/eng-377-linear-autoclose-promotion-model", "ENG-377"),
            ("docs/ENG-486-dw5-closeout", "ENG-486"),
        ] {
            assert_eq!(
                ticket_from_branch(&branch_of(name), &eng()).map(|t| t.to_string()),
                Some(want.to_owned()),
                "{name}"
            );
        }
    }

    #[test]
    fn a_branch_with_no_ticket_yields_none_rather_than_a_guess() {
        for name in [
            "dev",
            "chore/npm-audit-fix",
            "feature/rework-2-uploads",
            "feature/v2-uploads",
            "feature/eng-99999999999-overflow",
        ] {
            assert_eq!(ticket_from_branch(&branch_of(name), &eng()), None, "{name}");
        }
    }

    #[test]
    fn a_prefix_from_another_tracker_is_not_read_as_a_ticket() {
        let b = branch_of("feature/abc-123-something");
        assert_eq!(ticket_from_branch(&b, &eng()), None);
        assert_eq!(
            ticket_from_branch(&b, &["ABC".to_owned()]).map(|t| t.to_string()),
            Some("ABC-123".to_owned())
        );
    }
}
