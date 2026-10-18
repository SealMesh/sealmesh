//! Which rooms a checkout belongs to, from the project map the router sent.
//!
//! The map is the router's, not this laptop's: which project a repo is in
//! decides who may join its rooms, so it lives in the router's environment
//! and every laptop reads the one copy. What a daemon holds is the map cut
//! to the repos its own token can read — the router does the cutting, the
//! same way it does for a reader — so a project this machine has no readable
//! repo in never reaches it at all.
//!
//! Resolution names rooms and decides nothing. A room named here is joined,
//! when asked, through the ordinary join path, and the router's
//! `authorise_join` decides it; the ticket room in particular needs read
//! access to every repo in the project, which this map cannot tell.

use sealmesh_chat_wire::{RoomId, Scope};
use sealmesh_proto::ProjectEntry;
use sealmesh_wire::{RepoSlug, TicketId};

/// The rooms of a checkout on `repo`, with `ticket` from its branch: the
/// ticket room, then the repo room, then — only with `project_rooms` — the
/// project room, all in `repo`'s project and spelled as the map spells them.
///
/// Empty when `repo` is in no project of `projects`. That covers a repo in no
/// project, one in a project whose repos this machine cannot read, and an
/// empty map, and nothing here tells them apart (rule 25).
pub fn rooms_for(
    projects: &[ProjectEntry],
    repo: &RepoSlug,
    ticket: Option<&TicketId>,
    project_rooms: bool,
) -> Vec<RoomId> {
    let found = projects.iter().find_map(|project| {
        project
            .repos
            .iter()
            .find(|listed| listed.as_str().eq_ignore_ascii_case(repo.as_str()))
            .map(|listed| (project, listed))
    });
    let Some((project, listed)) = found else {
        return Vec::new();
    };
    let mut rooms = Vec::with_capacity(3);
    if let Some(ticket) = ticket {
        rooms.push(RoomId::new(
            project.id.clone(),
            Scope::Ticket(ticket.clone()),
        ));
    }
    rooms.push(RoomId::new(project.id.clone(), Scope::Repo(listed.clone())));
    if project_rooms {
        rooms.push(RoomId::new(project.id.clone(), Scope::Project));
    }
    rooms
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use sealmesh_wire::ProjectId;

    use super::*;

    fn entry(id: &str, repos: &[&str]) -> ProjectEntry {
        ProjectEntry {
            id: ProjectId::try_from(id).unwrap(),
            repos: repos
                .iter()
                .map(|r| RepoSlug::try_from(*r).unwrap())
                .collect(),
        }
    }

    fn map() -> Vec<ProjectEntry> {
        vec![
            entry(
                "example-repo",
                &[
                    "example-org/example-repo",
                    "example-org/Example-repo-Portal",
                ],
            ),
            entry("example-tools", &["example-org/example-tools"]),
        ]
    }

    fn slug(s: &str) -> RepoSlug {
        RepoSlug::try_from(s).unwrap()
    }

    fn eng(n: u32) -> TicketId {
        TicketId::new("ENG", n).unwrap()
    }

    fn example_repo(scope: Scope) -> RoomId {
        RoomId::new(ProjectId::try_from("example-repo").unwrap(), scope)
    }

    #[test]
    fn a_ticket_branch_resolves_to_its_ticket_room_then_its_repo_room() {
        assert_eq!(
            rooms_for(
                &map(),
                &slug("example-org/example-repo"),
                Some(&eng(7)),
                false
            ),
            vec![
                example_repo(Scope::Ticket(eng(7))),
                example_repo(Scope::Repo(slug("example-org/example-repo"))),
            ]
        );
    }

    #[test]
    fn no_ticket_is_the_repo_room_alone() {
        assert_eq!(
            rooms_for(&map(), &slug("example-org/example-repo"), None, false),
            vec![example_repo(Scope::Repo(slug("example-org/example-repo")))]
        );
    }

    #[test]
    fn the_project_room_comes_only_with_the_opt_in() {
        let rooms = rooms_for(
            &map(),
            &slug("example-org/example-repo"),
            Some(&eng(7)),
            true,
        );
        assert_eq!(rooms.last(), Some(&example_repo(Scope::Project)));
        assert_eq!(rooms.len(), 3);
        let rooms = rooms_for(
            &map(),
            &slug("example-org/example-repo"),
            Some(&eng(7)),
            false,
        );
        assert!(!rooms.contains(&example_repo(Scope::Project)), "{rooms:?}");
    }

    #[test]
    fn a_repo_is_matched_across_case_and_spelled_as_the_map_spells_it() {
        assert_eq!(
            rooms_for(
                &map(),
                &slug("Example-org/example-repo-portal"),
                None,
                false
            ),
            vec![example_repo(Scope::Repo(slug(
                "example-org/Example-repo-Portal"
            )))]
        );
    }

    #[test]
    fn a_repo_in_no_project_and_one_the_map_lacks_are_the_same_empty_answer() {
        let map = map();
        let unmapped = rooms_for(&map, &slug("example-org/sealmesh"), Some(&eng(7)), true);
        let unknown = rooms_for(&map, &slug("someone/else"), Some(&eng(7)), true);
        let no_map = rooms_for(&[], &slug("example-org/example-repo"), Some(&eng(7)), true);
        assert!(unmapped.is_empty());
        assert_eq!(unmapped, unknown);
        assert_eq!(unmapped, no_map);
    }

    #[test]
    fn a_ticket_never_names_a_project_the_repo_is_not_in() {
        // The ticket is only ever placed in the repo's own project.
        let rooms = rooms_for(
            &map(),
            &slug("example-org/example-tools"),
            Some(&eng(7)),
            false,
        );
        assert!(
            rooms
                .iter()
                .all(|r| r.project == ProjectId::try_from("example-tools").unwrap()),
            "{rooms:?}"
        );
    }
}
