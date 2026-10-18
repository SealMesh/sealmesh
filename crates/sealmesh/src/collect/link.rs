use url::Url;

use crate::wire::RepoSlug;

use super::job::RawChild;

pub use crate::gitref::ticket_from_branch;

pub fn pr_from_children(children: &[RawChild], repo: &RepoSlug) -> Option<u32> {
    children
        .iter()
        .filter(|child| child.kind.as_deref() == Some("pr"))
        .filter_map(|child| child.href.as_deref())
        .find_map(|href| pr_number(href, repo))
}

fn pr_number(href: &str, repo: &RepoSlug) -> Option<u32> {
    let url = Url::parse(href).ok()?;
    if url.scheme() != "https" {
        return None;
    }
    match url.host_str() {
        Some("github.com" | "www.github.com") => {}
        _ => return None,
    }
    let mut segments = url.path_segments()?;
    let owner = segments.next()?;
    let name = segments.next()?;
    let kind = segments.next()?;
    let number = segments.next()?;
    if segments.next().is_some() || kind != "pull" || owner != repo.owner() || name != repo.repo() {
        return None;
    }
    number.parse::<u32>().ok()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn example_repo() -> RepoSlug {
        RepoSlug::try_from("example-org/example-repo").unwrap()
    }

    #[test]
    fn only_a_pull_request_on_this_repo_survives() {
        let children = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(kind, href)| RawChild {
                    kind: Some((*kind).to_owned()),
                    href: Some((*href).to_owned()),
                })
                .collect::<Vec<_>>()
        };

        let mixed = children(&[
            ("frame", "https://claude.ai/code/artifact/209154f6"),
            ("pr", "https://github.com/example-org/sealmesh/pull/1"),
            ("pr", "https://github.com/example-org/example-repo/pull/627"),
        ]);
        assert_eq!(pr_from_children(&mixed, &example_repo()), Some(627));

        let elsewhere = children(&[("pr", "https://github.com/example-org/sealmesh/pull/1")]);
        assert_eq!(pr_from_children(&elsewhere, &example_repo()), None);
    }

    #[test]
    fn a_lookalike_host_or_path_is_refused() {
        for href in [
            "https://github.com.evil.test/example-org/example-repo/pull/627",
            "https://evil.test/github.com/example-org/example-repo/pull/627",
            "http://github.com/example-org/example-repo/pull/627",
            "https://github.com/example-org/example-repo/pull/627/files",
            "https://github.com/example-org/example-repo/issues/627",
            "https://github.com/someone-else/example-repo/pull/627",
            "javascript:alert(1)",
            "not a url at all",
        ] {
            assert_eq!(pr_number(href, &example_repo()), None, "accepted {href}");
        }
        assert_eq!(
            pr_number(
                "https://github.com/example-org/example-repo/pull/627",
                &example_repo()
            ),
            Some(627)
        );
    }
}
