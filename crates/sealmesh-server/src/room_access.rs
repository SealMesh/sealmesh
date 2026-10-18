//! Who may join a chat room.
//!
//! The rule lands before rooms exist so no room ever ships with a weaker
//! check. It is a function of the caller's own credential, the room's
//! [`ProjectId`](sealmesh_wire::ProjectId) and its [`Scope`], asked over the
//! [`Directory`] (in production the access cache over the GitHub resolver).
//! No transport, no registry: whether a room currently exists is never
//! consulted, which is what makes "denied" and "not found" one answer.
//!
//! # The decision table
//!
//! | Scope | Repos that must be `Readable` | Refused before GitHub is asked |
//! |---|---|---|
//! | `Project` (opt-in) | **every** repo the project lists | project not in the map |
//! | `Repo(r)` (automatic) | `r`, spelled as the map spells it | project not in the map; `r` not listed in *that* project |
//! | `Ticket(t)` (automatic) | **every** repo the project lists | project not in the map |
//!
//! Then, for the required set: an empty set refuses, any `NotReadable`
//! refuses, any `Unverifiable` refuses, and only a non-empty set that came
//! back `Readable` for every member grants. There is no path that grants on
//! the absence of a negative (core rule 38).
//!
//! # Why a ticket room is checked against every repo in its project
//!
//! A ticket room is keyed by project and ticket, and nothing else. A ticket
//! prefix names a Linear team, not a repository, so the repo cannot be derived
//! from the ticket. Two alternatives were on the table:
//!
//! - **The caller supplies the repo its branch is on, and the router verifies
//!   read access to it.** That verifies the wrong thing. The room is shared by
//!   every participant working that ticket in *any* of the project's repos, so
//!   a caller who can read one of them would be admitted to coordination about
//!   the others — the partial-access leak the project-scope rule exists to
//!   prevent, arriving by a narrower door. It also lets a client-supplied value
//!   decide what the caller is authorised to see (rule 12).
//! - **Every repo in the project.** Fails closed: a ticket room never admits
//!   anyone a project room would refuse. The cost is onboarding, not safety —
//!   in a multi-repo project, someone with access to only one repo cannot join
//!   that project's ticket rooms. In a single-repo project it is exactly "that
//!   one repo".
//!
//! This module takes the second. Narrowing it needs the repo to be part of the
//! room's identity (a `Scope::Ticket` that carries a repo), which is a wire
//! change for the chat-wire crate, not a looser check here.
//!
//! # Both entry paths take the same check
//!
//! Per Q3, ticket and repo rooms are joined automatically when a branch names
//! them and a project room is opt-in. [`authorise_join`] deliberately has no
//! parameter saying which path a request came from, so an opt-in join cannot be
//! waved through as "the user asked for it" and an automatic one cannot skip
//! the check as "the branch implied it".
//!
//! # What a refused caller learns
//!
//! Nothing about why (rule 25). Every refusal is [`Denied::NotAuthorised`]:
//! a project the router has never heard of, a repo outside the project, a repo
//! the caller cannot read and a GitHub outage are one answer. The reasons exist
//! only inside this module, for its own tests. The router no longer sends the
//! project map whole: each reader's `ServerFrame::Projects` reply lists only
//! the repos that reader can read. The map is still committed configuration
//! (`example.yaml`), so refusing an unmapped project without a GitHub call reveals
//! nothing that file does not. But that refusal is faster than a mapped
//! project's `can_read` round trip, which is a timing channel; making refusal
//! timing uniform belongs to the join route that calls this, not to this
//! function.
//!
//! The token is borrowed for the duration of the call and handed straight to
//! the directory; nothing here stores or formats it (rules 6, 7).

use sealmesh_chat_wire::{RoomId, Scope};
use sealmesh_proto::SecretToken;
use sealmesh_wire::RepoSlug;

use crate::{
    auth::{Denied, Directory, RepoAccess},
    projects::ProjectMap,
};

/// Why a join was refused. Private on purpose: it never reaches a caller, and
/// [`authorise_join`] collapses every variant into one [`Denied`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// The room names a project the router's map does not hold.
    UnmappedProject,
    /// A `Repo` scope names a repo that is not listed in the room's project —
    /// whether it is in another project or in none.
    RepoOutsideProject,
    /// Nothing to check. Unreachable through a map built by
    /// [`ProjectMap::parse`], which refuses an empty project, and refused
    /// anyway: "every repo is readable" over an empty set is vacuously true,
    /// and a vacuous truth is a grant on the absence of a negative.
    NoReposToCheck,
    /// GitHub answered, and for at least one required repo the answer is no.
    NotReadable,
    /// The check could not be completed for at least one required repo.
    Unverifiable,
}

/// The decision table: which repos must be readable to join `room`.
///
/// Every slug returned is the map's own spelling, never the caller's, so the
/// set that gets checked is the operator's configuration rather than a value
/// the client chose.
fn required_repos<'m>(projects: &'m ProjectMap, room: &RoomId) -> Result<&'m [RepoSlug], Refusal> {
    let listed = projects
        .repos_of(&room.project)
        .ok_or(Refusal::UnmappedProject)?;

    match &room.scope {
        Scope::Project | Scope::Ticket(_) => Ok(listed),
        Scope::Repo(named) => listed
            .iter()
            .find(|repo| repo.as_str().eq_ignore_ascii_case(named.as_str()))
            .map(std::slice::from_ref)
            .ok_or(Refusal::RepoOutsideProject),
    }
}

/// Grants only when `repos` is non-empty and every one of them is confirmed
/// `Readable` with the caller's own token. Stops at the first repo that is
/// not, so a refusal costs no more GitHub calls than it has to.
async fn every_repo_readable<D: Directory>(
    directory: &D,
    token: &SecretToken,
    repos: &[RepoSlug],
) -> Result<(), Refusal> {
    if repos.is_empty() {
        return Err(Refusal::NoReposToCheck);
    }
    for repo in repos {
        match directory.can_read(token, repo).await {
            RepoAccess::Readable => {}
            RepoAccess::NotReadable => return Err(Refusal::NotReadable),
            RepoAccess::Unverifiable => return Err(Refusal::Unverifiable),
        }
    }
    Ok(())
}

async fn decide<D: Directory>(
    directory: &D,
    projects: &ProjectMap,
    token: &SecretToken,
    room: &RoomId,
) -> Result<(), Refusal> {
    let repos = required_repos(projects, room)?;
    every_repo_readable(directory, token, repos).await
}

/// May the holder of `token` join `room`?
///
/// Default-deny: `Ok(())` only when the room's project is in `projects`, the
/// scope resolves to a non-empty set of that project's repos per the
/// [module table](self), and the directory confirms every one of them
/// `Readable` for this caller. Every other outcome — including one the
/// directory could not verify — is the same [`Denied::NotAuthorised`], so a
/// refused caller cannot tell a room it may not see from one that does not
/// exist.
pub async fn authorise_join<D: Directory>(
    directory: &D,
    projects: &ProjectMap,
    token: &SecretToken,
    room: &RoomId,
) -> Result<(), Denied> {
    decide(directory, projects, token, room)
        .await
        .map_err(|_refusal| Denied::NotAuthorised)
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use std::sync::Mutex;

    use sealmesh_wire::{ProjectId, TicketId};

    use super::*;

    const MAP: &str = r#"[
        {"id":"example-repo","repos":["example-org/example-repo","example-org/example-repo-portal"]},
        {"id":"example-tools","repos":["example-org/example-tools"]}
    ]"#;

    /// Answers `can_read` from a fixed table and records every repo it was
    /// asked about, so a test can prove which checks ran — and that a refusal
    /// decided by the map never reached GitHub at all.
    struct Table {
        answers: Vec<(&'static str, RepoAccess)>,
        asked: Mutex<Vec<String>>,
    }

    impl Table {
        fn new(answers: &[(&'static str, RepoAccess)]) -> Self {
            Self {
                answers: answers.to_vec(),
                asked: Mutex::new(Vec::new()),
            }
        }

        fn asked(&self) -> Vec<String> {
            self.asked.lock().unwrap().clone()
        }
    }

    impl Directory for Table {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn can_read(&self, _token: &SecretToken, repo: &RepoSlug) -> RepoAccess {
            self.asked.lock().unwrap().push(repo.as_str().to_owned());
            // Exact spelling on purpose: a test that expects a grant only gets
            // one if the check asked with the map's spelling. A repo the table
            // does not list is the trait's own fail-closed default.
            self.answers
                .iter()
                .find(|(slug, _)| *slug == repo.as_str())
                .map_or(RepoAccess::Unverifiable, |(_, access)| *access)
        }
    }

    fn token() -> SecretToken {
        SecretToken::new("ghp_example".to_owned())
    }

    fn map() -> ProjectMap {
        ProjectMap::parse(MAP).unwrap()
    }

    fn room(project: &str, scope: Scope) -> RoomId {
        RoomId::new(ProjectId::try_from(project).unwrap(), scope)
    }

    fn repo(slug: &str) -> Scope {
        Scope::Repo(RepoSlug::try_from(slug).unwrap())
    }

    fn ticket() -> Scope {
        Scope::Ticket(TicketId::new("ENG", 557).unwrap())
    }

    use RepoAccess::{NotReadable as No, Readable as Yes, Unverifiable as Outage};

    struct Case {
        name: &'static str,
        room: RoomId,
        access: &'static [(&'static str, RepoAccess)],
        want: Result<(), Refusal>,
        /// Every repo the check must have asked about, in order.
        asked: &'static [&'static str],
    }

    const EXAMPLE_REPO: &str = "example-org/example-repo";
    const PORTAL: &str = "example-org/example-repo-portal";
    const SKILLS: &str = "example-org/example-tools";

    fn cases() -> Vec<Case> {
        vec![
            // Automatic entry path: a branch names a repo.
            Case {
                name: "automatic repo join, readable repo in the project",
                room: room("example-repo", repo(PORTAL)),
                access: &[(PORTAL, Yes), (EXAMPLE_REPO, No)],
                want: Ok(()),
                asked: &[PORTAL],
            },
            Case {
                name: "automatic repo join, repo the caller cannot read",
                room: room("example-repo", repo(PORTAL)),
                access: &[(PORTAL, No), (EXAMPLE_REPO, Yes)],
                want: Err(Refusal::NotReadable),
                asked: &[PORTAL],
            },
            Case {
                name: "automatic repo join, GitHub could not be asked",
                room: room("example-repo", repo(PORTAL)),
                access: &[(PORTAL, Outage)],
                want: Err(Refusal::Unverifiable),
                asked: &[PORTAL],
            },
            Case {
                name: "automatic repo join, readable repo from a different project",
                room: room("example-repo", repo(SKILLS)),
                access: &[(SKILLS, Yes)],
                want: Err(Refusal::RepoOutsideProject),
                asked: &[],
            },
            Case {
                name: "automatic repo join, readable repo in no project",
                room: room("example-repo", repo("example-org/sealmesh")),
                access: &[("example-org/sealmesh", Yes)],
                want: Err(Refusal::RepoOutsideProject),
                asked: &[],
            },
            Case {
                name: "automatic repo join, caller's spelling differs, map's spelling is checked",
                room: room("example-repo", repo("Example-org/Example-repo-Portal")),
                access: &[(PORTAL, Yes)],
                want: Ok(()),
                asked: &[PORTAL],
            },
            Case {
                name: "automatic repo join, project the router has never heard of",
                room: room("ghost", repo(EXAMPLE_REPO)),
                access: &[(EXAMPLE_REPO, Yes)],
                want: Err(Refusal::UnmappedProject),
                asked: &[],
            },
            // Automatic entry path: a branch names a ticket.
            Case {
                name: "automatic ticket join, every repo in the project readable",
                room: room("example-repo", ticket()),
                access: &[(EXAMPLE_REPO, Yes), (PORTAL, Yes)],
                want: Ok(()),
                asked: &[EXAMPLE_REPO, PORTAL],
            },
            Case {
                name: "automatic ticket join, the branch's own repo readable but not the other",
                room: room("example-repo", ticket()),
                access: &[(EXAMPLE_REPO, Yes), (PORTAL, No)],
                want: Err(Refusal::NotReadable),
                asked: &[EXAMPLE_REPO, PORTAL],
            },
            Case {
                name: "automatic ticket join, one repo unverifiable",
                room: room("example-repo", ticket()),
                access: &[(EXAMPLE_REPO, Yes), (PORTAL, Outage)],
                want: Err(Refusal::Unverifiable),
                asked: &[EXAMPLE_REPO, PORTAL],
            },
            Case {
                name: "automatic ticket join, single-repo project is exactly that one repo",
                room: room("example-tools", ticket()),
                access: &[(SKILLS, Yes)],
                want: Ok(()),
                asked: &[SKILLS],
            },
            Case {
                name: "automatic ticket join, project the router has never heard of",
                room: room("ghost", ticket()),
                access: &[(EXAMPLE_REPO, Yes), (PORTAL, Yes)],
                want: Err(Refusal::UnmappedProject),
                asked: &[],
            },
            // Opt-in entry path: the project room.
            Case {
                name: "opt-in project join, every repo readable",
                room: room("example-repo", Scope::Project),
                access: &[(EXAMPLE_REPO, Yes), (PORTAL, Yes)],
                want: Ok(()),
                asked: &[EXAMPLE_REPO, PORTAL],
            },
            Case {
                name: "opt-in project join, one missing repo denies the whole join",
                room: room("example-repo", Scope::Project),
                access: &[(EXAMPLE_REPO, Yes), (PORTAL, No)],
                want: Err(Refusal::NotReadable),
                asked: &[EXAMPLE_REPO, PORTAL],
            },
            Case {
                name: "opt-in project join, first repo missing stops the check",
                room: room("example-repo", Scope::Project),
                access: &[(EXAMPLE_REPO, No), (PORTAL, Yes)],
                want: Err(Refusal::NotReadable),
                asked: &[EXAMPLE_REPO],
            },
            Case {
                name: "opt-in project join, one repo unverifiable",
                room: room("example-repo", Scope::Project),
                access: &[(EXAMPLE_REPO, Outage), (PORTAL, Yes)],
                want: Err(Refusal::Unverifiable),
                asked: &[EXAMPLE_REPO],
            },
            Case {
                name: "opt-in project join, a repo the directory knows nothing about",
                room: room("example-repo", Scope::Project),
                access: &[(EXAMPLE_REPO, Yes)],
                want: Err(Refusal::Unverifiable),
                asked: &[EXAMPLE_REPO, PORTAL],
            },
            Case {
                name: "opt-in project join, project the router has never heard of",
                room: room("ghost", Scope::Project),
                access: &[(EXAMPLE_REPO, Yes), (PORTAL, Yes)],
                want: Err(Refusal::UnmappedProject),
                asked: &[],
            },
        ]
    }

    #[tokio::test]
    async fn the_decision_table() {
        let projects = map();
        for case in cases() {
            let directory = Table::new(case.access);
            let got = decide(&directory, &projects, &token(), &case.room).await;
            assert_eq!(got, case.want, "{}", case.name);
            assert_eq!(directory.asked(), case.asked, "{}: asked", case.name);
        }
    }

    #[tokio::test]
    async fn every_refusal_is_the_same_answer_to_the_caller() {
        let projects = map();
        let mut refused = 0;
        for case in cases() {
            let directory = Table::new(case.access);
            let got = authorise_join(&directory, &projects, &token(), &case.room).await;
            match case.want {
                Ok(()) => assert_eq!(got, Ok(()), "{}", case.name),
                Err(_) => {
                    refused += 1;
                    assert_eq!(got, Err(Denied::NotAuthorised), "{}", case.name);
                }
            }
        }
        // The table covers every refusal reason reachable through a parsed map.
        assert!(refused >= 4, "only {refused} refusals exercised");
    }

    #[tokio::test]
    async fn a_room_that_does_not_exist_and_a_room_the_caller_cannot_read_look_identical() {
        let projects = map();
        let nobody_can_read = Table::new(&[(EXAMPLE_REPO, No), (PORTAL, No)]);
        let not_found = authorise_join(
            &nobody_can_read,
            &projects,
            &token(),
            &room("ghost", Scope::Project),
        )
        .await;
        let not_readable = authorise_join(
            &nobody_can_read,
            &projects,
            &token(),
            &room("example-repo", Scope::Project),
        )
        .await;
        let outage = authorise_join(
            &Table::new(&[(EXAMPLE_REPO, Outage), (PORTAL, Outage)]),
            &projects,
            &token(),
            &room("example-repo", Scope::Project),
        )
        .await;
        assert_eq!(not_found, Err(Denied::NotAuthorised));
        assert_eq!(not_found, not_readable);
        assert_eq!(not_readable, outage);
    }

    #[tokio::test]
    async fn an_empty_map_admits_nobody_to_any_scope() {
        let directory = Table::new(&[(EXAMPLE_REPO, Yes), (PORTAL, Yes)]);
        for scope in [Scope::Project, ticket(), repo(EXAMPLE_REPO)] {
            assert_eq!(
                decide(
                    &directory,
                    &ProjectMap::empty(),
                    &token(),
                    &room("example-repo", scope)
                )
                .await,
                Err(Refusal::UnmappedProject)
            );
        }
        assert!(directory.asked().is_empty());
    }

    #[tokio::test]
    async fn an_empty_set_of_repos_is_refused_rather_than_vacuously_granted() {
        let directory = Table::new(&[]);
        assert_eq!(
            every_repo_readable(&directory, &token(), &[]).await,
            Err(Refusal::NoReposToCheck)
        );
    }

    #[tokio::test]
    async fn a_directory_that_cannot_resolve_access_admits_nobody() {
        /// Uses the trait's default `can_read`, which is `Unverifiable`.
        struct Untaught;
        impl Directory for Untaught {
            async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
                Ok("sandbank".to_owned())
            }
            async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
                Ok(true)
            }
        }

        let projects = map();
        for scope in [Scope::Project, ticket(), repo(EXAMPLE_REPO)] {
            assert_eq!(
                authorise_join(&Untaught, &projects, &token(), &room("example-repo", scope)).await,
                Err(Denied::NotAuthorised)
            );
        }
    }
}
