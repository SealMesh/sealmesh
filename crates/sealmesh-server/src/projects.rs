use std::collections::{BTreeMap, btree_map::Entry};

use sealmesh_wire::{ProjectId, RepoSlug};
use serde::Deserialize;

pub const PROJECTS_VAR: &str = "SEALMESH_PROJECTS";

pub const MAX_PROJECTS_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProjectsError {
    #[error("{PROJECTS_VAR} is {0} bytes, over the {MAX_PROJECTS_BYTES}-byte limit")]
    TooLarge(usize),
    #[error(
        "{PROJECTS_VAR} is not a list of {{\"id\", \"repos\"}} projects (line {line}, column {column})"
    )]
    Json { line: usize, column: usize },
    #[error("project {0} is declared twice")]
    DuplicateProject(ProjectId),
    #[error("project {0} lists no repos")]
    EmptyProject(ProjectId),
    #[error("project {project} lists {repo} twice")]
    RepoRepeated { project: ProjectId, repo: RepoSlug },
    #[error("{repo} is in both {first} and {second}, and a repo belongs to at most one project")]
    RepoInTwoProjects {
        repo: RepoSlug,
        first: ProjectId,
        second: ProjectId,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProject {
    id: ProjectId,
    repos: Vec<RepoSlug>,
}

/// Which project each repo belongs to. Built only from the router's own
/// environment, and only whole: any problem refuses the entire map, because a
/// half-loaded one silently changes who can join what.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectMap {
    by_repo: BTreeMap<String, ProjectId>,
    projects: BTreeMap<ProjectId, Vec<RepoSlug>>,
}

impl ProjectMap {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn parse(value: &str) -> Result<Self, ProjectsError> {
        if value.len() > MAX_PROJECTS_BYTES {
            return Err(ProjectsError::TooLarge(value.len()));
        }
        let raw: Vec<RawProject> =
            serde_json::from_str(value).map_err(|e| ProjectsError::Json {
                line: e.line(),
                column: e.column(),
            })?;

        let mut map = Self::default();
        for project in raw {
            if map.projects.contains_key(&project.id) {
                return Err(ProjectsError::DuplicateProject(project.id));
            }
            if project.repos.is_empty() {
                return Err(ProjectsError::EmptyProject(project.id));
            }
            for repo in &project.repos {
                match map.by_repo.entry(repo_key(repo)) {
                    Entry::Vacant(slot) => {
                        slot.insert(project.id.clone());
                    }
                    Entry::Occupied(slot) if *slot.get() == project.id => {
                        return Err(ProjectsError::RepoRepeated {
                            project: project.id.clone(),
                            repo: repo.clone(),
                        });
                    }
                    Entry::Occupied(slot) => {
                        return Err(ProjectsError::RepoInTwoProjects {
                            repo: repo.clone(),
                            first: slot.get().clone(),
                            second: project.id.clone(),
                        });
                    }
                }
            }
            map.projects.insert(project.id, project.repos);
        }
        Ok(map)
    }

    /// `None` is a legal answer: a repo in no project gets no rooms at all.
    /// There is no per-repo fallback room — every `RoomId` carries a
    /// project — so a repo outside the map cannot join, post to, or list
    /// any room.
    pub fn project_of(&self, repo: &RepoSlug) -> Option<&ProjectId> {
        self.by_repo.get(&repo_key(repo))
    }

    /// The repos `project` lists, spelled as the environment spelled them.
    /// `None` for a project the map does not hold. Never an empty slice for a
    /// parsed map, since `parse` refuses a project with no repos.
    pub fn repos_of(&self, project: &ProjectId) -> Option<&[RepoSlug]> {
        self.projects.get(project).map(Vec::as_slice)
    }

    /// A read-only view of the map in the shape `ServerFrame::Projects`
    /// carries. `id` and `repos` already went through their validating
    /// constructors when the map was built, so there is nothing left to
    /// check here — this only reshapes what is already trusted.
    pub fn to_wire(&self) -> Vec<sealmesh_proto::ProjectEntry> {
        self.projects
            .iter()
            .map(|(id, repos)| sealmesh_proto::ProjectEntry {
                id: id.clone(),
                repos: repos.clone(),
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.projects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.projects.is_empty()
    }
}

/// GitHub treats repo names case-insensitively, so the map does too. Without
/// this, one repo spelled two ways could sit in two projects undetected.
fn repo_key(repo: &RepoSlug) -> String {
    repo.as_str().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn repo(s: &str) -> RepoSlug {
        RepoSlug::try_from(s).unwrap()
    }

    fn project(s: &str) -> ProjectId {
        ProjectId::try_from(s).unwrap()
    }

    /// A two-project mapping in the shape a router is deployed with: one
    /// project spanning two repos, one with a single repo.
    fn deployed_value() -> &'static str {
        r#"[{"id":"example-repo","repos":["example-org/example-repo","example-org/example-repo-portal"]},{"id":"example-tools","repos":["example-org/example-tools"]}]"#
    }

    #[test]
    fn the_deployed_mapping_parses_and_places_its_repos() {
        let map = ProjectMap::parse(deployed_value()).unwrap();
        assert_eq!(
            map.project_of(&repo("example-org/example-repo")),
            Some(&project("example-repo"))
        );
        assert_eq!(
            map.project_of(&repo("example-org/example-repo-portal")),
            Some(&project("example-repo"))
        );
        assert_eq!(
            map.project_of(&repo("example-org/example-tools")),
            Some(&project("example-tools"))
        );
    }

    #[test]
    fn a_repo_in_no_project_is_legal_and_has_no_project() {
        let map = ProjectMap::parse(deployed_value()).unwrap();
        assert_eq!(map.project_of(&repo("example-org/sealmesh")), None);
    }

    #[test]
    fn repos_of_lists_a_projects_repos_and_nothing_for_an_unknown_project() {
        let map = ProjectMap::parse(
            r#"[{"id":"example-repo","repos":["example-org/Example-repo","example-org/example-repo-portal"]}]"#,
        )
        .unwrap();
        assert_eq!(
            map.repos_of(&project("example-repo")),
            Some(
                [
                    repo("example-org/Example-repo"),
                    repo("example-org/example-repo-portal")
                ]
                .as_slice()
            )
        );
        assert_eq!(map.repos_of(&project("ghost")), None);
        assert_eq!(ProjectMap::empty().repos_of(&project("example-repo")), None);
    }

    #[test]
    fn an_explicit_empty_list_is_an_empty_map() {
        let map = ProjectMap::parse("[]").unwrap();
        assert!(map.is_empty());
        assert_eq!(map, ProjectMap::empty());
    }

    #[test]
    fn lookup_ignores_the_case_github_ignores() {
        let map =
            ProjectMap::parse(r#"[{"id":"example-repo","repos":["example-org/Example-repo"]}]"#)
                .unwrap();
        assert_eq!(
            map.project_of(&repo("example-org/example-repo")),
            Some(&project("example-repo"))
        );
    }

    #[test]
    fn a_malformed_mapping_refuses_rather_than_loading_part_of_it() {
        for hostile in [
            "",
            "not json",
            "{}",
            r#"{"id":"example-repo","repos":["example-org/example-repo"]}"#,
            r#"[{"id":"example-repo","repos":["example-org/example-repo"],"owner":"x"}]"#,
            r#"[{"id":"Example-repo","repos":["example-org/example-repo"]}]"#,
            r#"[{"id":"example-repo","repos":["example-repo"]}]"#,
            r#"[{"id":"example-repo"}]"#,
            r#"[{"id":"example-repo","repos":["example-org/example-repo"]},{"id":"Bad","repos":["example-org/x"]}]"#,
        ] {
            assert!(
                matches!(ProjectMap::parse(hostile), Err(ProjectsError::Json { .. })),
                "accepted {hostile:?}"
            );
        }
    }

    #[test]
    fn a_repo_in_two_projects_is_refused_not_resolved_by_precedence() {
        assert_eq!(
            ProjectMap::parse(
                r#"[{"id":"example-repo","repos":["example-org/shared"]},{"id":"example-tools","repos":["example-org/shared"]}]"#
            ),
            Err(ProjectsError::RepoInTwoProjects {
                repo: repo("example-org/shared"),
                first: project("example-repo"),
                second: project("example-tools"),
            })
        );
    }

    #[test]
    fn one_repo_spelled_two_ways_is_still_in_two_projects() {
        assert!(matches!(
            ProjectMap::parse(
                r#"[{"id":"example-repo","repos":["example-org/shared"]},{"id":"example-tools","repos":["example-org/Shared"]}]"#
            ),
            Err(ProjectsError::RepoInTwoProjects { .. })
        ));
    }

    #[test]
    fn a_project_declared_twice_is_refused() {
        assert_eq!(
            ProjectMap::parse(
                r#"[{"id":"example-repo","repos":["example-org/example-repo"]},{"id":"example-repo","repos":["example-org/example-repo-portal"]}]"#
            ),
            Err(ProjectsError::DuplicateProject(project("example-repo")))
        );
    }

    #[test]
    fn a_project_with_no_repos_is_refused() {
        assert_eq!(
            ProjectMap::parse(r#"[{"id":"example-repo","repos":[]}]"#),
            Err(ProjectsError::EmptyProject(project("example-repo")))
        );
    }

    #[test]
    fn a_repo_listed_twice_in_one_project_is_refused() {
        for listed in [
            r#"[{"id":"example-repo","repos":["example-org/example-repo","example-org/example-repo"]}]"#,
            r#"[{"id":"example-repo","repos":["example-org/example-repo","example-org/EXAMPLE-REPO"]}]"#,
        ] {
            assert!(
                matches!(
                    ProjectMap::parse(listed),
                    Err(ProjectsError::RepoRepeated { .. })
                ),
                "accepted {listed}"
            );
        }
    }

    #[test]
    fn an_oversized_mapping_is_refused_before_the_parser_sees_it() {
        let oversized = "x".repeat(MAX_PROJECTS_BYTES + 1);
        assert_eq!(
            ProjectMap::parse(&oversized),
            Err(ProjectsError::TooLarge(MAX_PROJECTS_BYTES + 1))
        );
    }

    #[test]
    fn to_wire_reflects_the_deployed_mapping() {
        let map = ProjectMap::parse(deployed_value()).unwrap();
        let wire = map.to_wire();
        assert_eq!(wire.len(), map.len());
        let example_repo = wire
            .iter()
            .find(|entry| entry.id == project("example-repo"))
            .expect("example-repo is in the deployed mapping");
        assert!(
            example_repo
                .repos
                .contains(&repo("example-org/example-repo"))
        );
        assert!(
            example_repo
                .repos
                .contains(&repo("example-org/example-repo-portal"))
        );
    }

    #[test]
    fn an_empty_map_serialises_to_an_empty_wire_list() {
        assert!(ProjectMap::empty().to_wire().is_empty());
    }

    /// The env var is capped at `MAX_PROJECTS_BYTES` before it is ever parsed
    /// (rule 42), and `ProjectEntry` carries the same two fields as
    /// `RawProject`, so a maximal mapping should re-encode to roughly the same
    /// size — comfortably under `MAX_FRAME_BYTES`, not merely under some
    /// number picked to make this test pass.
    #[test]
    fn a_maximal_mapping_fits_in_one_frame() {
        // One project with as many distinctly-named repos as fit under the
        // env var's own byte cap.
        let mut repos = String::new();
        let mut n = 0usize;
        loop {
            let candidate = format!(r#""example-org/repo-{n}","#);
            if repos.len() + candidate.len() + 32 > MAX_PROJECTS_BYTES {
                break;
            }
            repos.push_str(&candidate);
            n += 1;
        }
        repos.pop(); // trailing comma
        let value = format!(r#"[{{"id":"big","repos":[{repos}]}}]"#);
        assert!(value.len() <= MAX_PROJECTS_BYTES, "{}", value.len());

        let map = ProjectMap::parse(&value).unwrap();
        let wire = map.to_wire();
        let encoded =
            serde_json::to_vec(&sealmesh_proto::ServerFrame::Projects { projects: wire }).unwrap();
        assert!(
            encoded.len() < sealmesh_proto::MAX_FRAME_BYTES,
            "a maximal projects reply is {} bytes, over MAX_FRAME_BYTES ({})",
            encoded.len(),
            sealmesh_proto::MAX_FRAME_BYTES
        );
    }

    #[test]
    fn a_refusal_names_the_variable() {
        for refused in [
            ProjectMap::parse("not json").unwrap_err(),
            ProjectMap::parse(&"x".repeat(MAX_PROJECTS_BYTES + 1)).unwrap_err(),
        ] {
            let message = refused.to_string();
            assert!(message.contains(PROJECTS_VAR), "{message}");
        }
    }
}
