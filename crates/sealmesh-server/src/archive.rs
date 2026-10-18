//! Archive a ticket room once its pull request merges.
//!
//! A ticket room scopes one piece of work, and once that work has merged the
//! room has nothing left to coordinate. Archiving closes it to writes and does
//! nothing else: joins still work, its history is still readable, nothing is
//! deleted, and the history's seven-day TTL is what removes the content —
//! deleting on merge would destroy the record at exactly the moment someone
//! might want to review how the coordination went.
//!
//! # Where the merge signal comes from
//!
//! From GitHub, asked by the router, **on demand, with the credential of the
//! connection posting to the room** — the token that connection's own task
//! already holds for its joins and re-checks, borrowed for the call and never
//! stored, copied into a shared structure or logged. Three alternatives lost:
//!
//! - **An agent saying its PR merged.** A client-supplied fact that would close
//!   a room other people are writing in (rule 12). There is no frame field
//!   that could carry one, and nothing here takes a pull request number: the
//!   pull request is found from the ticket id in the room's key, within the
//!   repos the router's own project map lists for the room's project.
//! - **A background poller.** It would need a credential held indefinitely,
//!   independent of any connection, which this router deliberately does not
//!   have.
//! - **A webhook.** A new public route, and with it a signature secret and
//!   rules 1 and 20 to defend, for a signal nobody needs until somebody posts.
//!
//! So a room is archived at the first post after its merge, not at the
//! merge. A room nobody posts to again is never marked archived — and nothing
//! is lost by that, because nobody is trying to write to it.
//!
//! # Which pull request is the ticket's
//!
//! One whose head branch is the ticket's: the **first** ticket-shaped token
//! in the branch, at the start of a path segment, is the ticket's id,
//! case-insensitively — `feature/xyz-62-archive` and `xyz-62` are XYZ-62's;
//! `xyz-620-x`, `axyz-62` and `fix-xyz-62` are not. That is the branch Linear
//! hands out for a ticket. Only the first token counts because Linear appends
//! the ticket's slugified title, and a title can name another ticket:
//! `feature/xyz-63-follow-up-on-xyz-62` is XYZ-63's branch. Reading any
//! occurrence would let XYZ-63's merge close XYZ-62's room. Pull request
//! titles are not read, for the same reason.
//!
//! A room is archived when at least one of the ticket's pull requests across
//! the project's repos has merged **into its repository's default branch**
//! and none is still open. A pull request merged into another branch is a
//! part of a stack landing on its parent, not the work landing, and counts
//! for nothing. A ticket landed as a stack is still in progress while any
//! part is open.
//!
//! # Fail open, distinctly (core rule 38)
//!
//! A check has three kinds of outcome, not two. `Merged` archives.
//! `InProgress` and `NotMerged` — GitHub answered, and a pull request is
//! still open, or none has merged — leave the room open and are cached for
//! [`NOT_MERGED_TTL`].
//! `Unverifiable` — a network error, a timeout, a rate limit, a repository the
//! token cannot list, more repos than [`MAX_REPOS_PER_CHECK`], or the whole
//! check running past its deadline — also leaves the room open, is logged,
//! and is cached only for [`UNVERIFIABLE_RETRY`], so an outage costs each
//! room one waiting post every half minute rather than every post. It is never
//! read as `Merged`: closing a room on a failed read would let one API hiccup
//! silence a room that is still in use.
//!
//! # Off the ingest lock, and off the connection's critical path
//!
//! The GitHub call happens before a post takes the process-wide ingest lock,
//! and the ingest path only reads the verdict this module already recorded —
//! so a slow GitHub delays the one post that asked, never posting in another
//! room. And the post is itself a pinned future in the connection's `select!`
//! (`http::serve_daemon`), so the wait never stops that connection answering
//! a query either.
//!
//! # What a check may cost
//!
//! At most [`MAX_REPOS_PER_CHECK`] GraphQL requests, one per repo in the
//! room's project, each capped at `github::RECENT_PULLS` pull requests and
//! the router's response byte cap, all inside [`CHECK_BUDGET`]. A check runs
//! only for a ticket room the posting connection is a member of — a post
//! naming any other room asks GitHub nothing (rule 44) — and at most once per
//! [`NOT_MERGED_TTL`] per room while the answer is no, because the verdict is
//! per room and shared by every member. It is the room's state, not anyone's
//! access: every member passed the same join check against the same repos.
//!
//! # An archive holds, with one way back
//!
//! Archiving an archived room changes nothing. An archived room is asked again
//! at most every [`ARCHIVED_RECHECK`], and only one answer reopens it: a pull
//! request for the ticket that is **open** now — the second part of a stacked
//! ticket, opened after the first merged. "No merged pull request found" does
//! not reopen it, because that is also what a merge looks like once it has
//! scrolled out of the listing, and neither does an unverifiable check. So a
//! room does not flap on an API hiccup, and a stale answer cannot reopen a
//! room a newer check archived.
//!
//! Like every room, the verdict is in-process, so a deploy forgets it and the
//! next post re-checks. That re-check, and one after the table's
//! [`MAX_VERDICTS`] cap evicts an archived room, finds the same merge only
//! while it is still among the repo's `github::RECENT_PULLS` most recently
//! updated pull requests. Past that window the room reads as not merged and
//! is open again — the fail-open direction.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use sealmesh_chat_wire::RoomId;
use sealmesh_proto::SecretToken;
use sealmesh_wire::{RepoSlug, TicketId};

use crate::auth::{Directory, PullState, PullSummary};

/// Most repos one merge check may ask about — one request each.
///
/// A project maps to a handful of repos, and a check runs while a post waits.
/// A project with more than this is not checked partially: an open pull
/// request in an unchecked repo would be missed, so it is `Unverifiable` and
/// its ticket rooms stay open.
pub const MAX_REPOS_PER_CHECK: usize = 8;

/// Wall-clock budget for one whole merge check. The same ten seconds a join's
/// access check gets (`rooms::JOIN_BUDGET`). A check still running at the
/// deadline is `Unverifiable`, which leaves the room open.
pub const CHECK_BUDGET: Duration = Duration::from_secs(10);

/// How long "not merged" is believed before a post asks again.
///
/// This bounds how late an archive lands after the merge: at most a minute
/// past the first post that follows it. It also bounds the cost of a busy
/// room to one check a minute, however many members post in it.
pub const NOT_MERGED_TTL: Duration = Duration::from_secs(60);

/// How long an unverifiable check is remembered before a post asks again.
///
/// Remembered at all so that a GitHub outage does not make every post into a
/// ticket room wait out [`CHECK_BUDGET`] first. Remembered as its own state,
/// never as "not merged", and shorter than [`NOT_MERGED_TTL`], so the room is
/// re-asked sooner once GitHub answers again.
pub const UNVERIFIABLE_RETRY: Duration = Duration::from_secs(30);

/// How often an archived room is asked again, to find a newly opened pull
/// request for its ticket. Only a post triggers it, so a quiet archived room
/// costs nothing; a busy one costs one check every five minutes.
pub const ARCHIVED_RECHECK: Duration = Duration::from_secs(300);

/// Most rooms whose verdict is held at once (rule 44). Far above the ticket
/// rooms two engineers work in over a router's life, and still a bound.
pub const MAX_VERDICTS: usize = 1024;

/// What one check concluded about one ticket room.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeVerdict {
    /// A pull request for the ticket merged into its default branch, and none
    /// is open.
    Merged,
    /// GitHub answered, and a pull request for the ticket is open. The only
    /// verdict that reopens an archived room.
    InProgress,
    /// GitHub answered, and there is no pull request for the ticket merged
    /// into a default branch and none open.
    NotMerged,
    /// The check could not be completed.
    Unverifiable,
}

/// Whether `head_ref` is a branch for `ticket`: see the module docs.
///
/// The ticket is the **first** ticket-shaped token in the branch, taken at
/// the start of a `/`-separated segment. Leading segments that do not start
/// with one (`feature/`, `sandbank/`) are skipped; the first segment that does
/// decides, and nothing after it is read. That matters because Linear's
/// branch names carry the ticket's slugified title after its id, and a title
/// can name another ticket: `xyz-63-follow-up-on-xyz-62-retention` is XYZ-63's
/// branch, and must never read as XYZ-62's.
pub fn names_ticket(head_ref: &str, ticket: &TicketId) -> bool {
    let wanted = ticket.to_string().to_ascii_lowercase();
    head_ref
        .split('/')
        .find_map(leading_ticket)
        .is_some_and(|found| found.eq_ignore_ascii_case(&wanted))
}

/// The ticket-shaped token `segment` starts with, if any: 2 to 10 ASCII
/// letters, a `-`, 1 to 6 digits, and then the end or a character that is not
/// a letter or digit. The same shape [`TicketId`] accepts, without its case.
fn leading_ticket(segment: &str) -> Option<&str> {
    let letters = segment.bytes().take_while(u8::is_ascii_alphabetic).count();
    if !(2..=10).contains(&letters) || segment.as_bytes().get(letters) != Some(&b'-') {
        return None;
    }
    let digits_from = letters.saturating_add(1);
    let digits = segment
        .bytes()
        .skip(digits_from)
        .take_while(u8::is_ascii_digit)
        .count();
    if !(1..=6).contains(&digits) {
        return None;
    }
    let end = digits_from.saturating_add(digits);
    if segment
        .as_bytes()
        .get(end)
        .is_some_and(u8::is_ascii_alphanumeric)
    {
        return None;
    }
    // Every byte before `end` is ASCII, so this cannot split a character.
    segment.get(..end)
}

/// Asks GitHub, with `token`, whether `ticket` has merged in `repos`.
///
/// `repos` is the router's own list for the room's project, never anything a
/// client sent. Bounded by [`MAX_REPOS_PER_CHECK`] and `budget`; see the
/// module docs.
pub async fn check_merged<D: Directory>(
    directory: &D,
    token: &SecretToken,
    repos: &[RepoSlug],
    ticket: &TicketId,
    budget: Duration,
) -> MergeVerdict {
    // Nothing to ask is no evidence of a merge. Unreachable through a parsed
    // project map, which refuses an empty project.
    if repos.is_empty() || repos.len() > MAX_REPOS_PER_CHECK {
        return MergeVerdict::Unverifiable;
    }
    tokio::time::timeout(budget, ask_each(directory, token, repos, ticket))
        .await
        .unwrap_or(MergeVerdict::Unverifiable)
}

async fn ask_each<D: Directory>(
    directory: &D,
    token: &SecretToken,
    repos: &[RepoSlug],
    ticket: &TicketId,
) -> MergeVerdict {
    let mut merged = false;
    for repo in repos {
        let Ok(pulls) = directory.recent_pulls(token, repo).await else {
            return MergeVerdict::Unverifiable;
        };
        match deciding_state(&pulls, ticket) {
            // An open pull request anywhere is a confirmed "not yet", whatever
            // the other repos would say.
            Some(PullState::Open) => return MergeVerdict::InProgress,
            Some(PullState::Merged) => merged = true,
            Some(PullState::Closed) | None => {}
        }
    }
    if merged {
        MergeVerdict::Merged
    } else {
        MergeVerdict::NotMerged
    }
}

/// The deciding state among one repo's pull requests for `ticket`: `Open` if
/// any is open, else `Merged` if any merged **into the default branch**, else
/// `Closed` if any was closed or merged elsewhere, else `None`.
///
/// A pull request merged into another branch — the part below it in a stack —
/// is `MERGED` to GitHub but has not landed the work, so it counts only as
/// closed.
fn deciding_state(pulls: &[PullSummary], ticket: &TicketId) -> Option<PullState> {
    let mut decided = None;
    for pull in pulls
        .iter()
        .filter(|pull| names_ticket(&pull.head_ref, ticket))
    {
        let state = match pull.state {
            PullState::Merged if !pull.into_default_branch => PullState::Closed,
            state => state,
        };
        decided = match (state, decided) {
            (PullState::Open, _) => return Some(PullState::Open),
            (PullState::Merged, _) | (PullState::Closed, Some(PullState::Merged)) => {
                Some(PullState::Merged)
            }
            (PullState::Closed, _) => Some(PullState::Closed),
        };
    }
    decided
}

/// A verdict as held between checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Known {
    /// Closed to writes. Asked again at `recheck_at`, and reopened only by a
    /// confirmed open pull request for the ticket — see [`Verdicts::record`].
    Archived {
        recheck_at: Instant,
    },
    NotMergedUntil(Instant),
    UnverifiableUntil(Instant),
}

struct Entry {
    known: Known,
    /// When the check that produced `known` started.
    recorded_at: Instant,
}

/// What recording a verdict changed, for the caller to log once it holds no
/// lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recorded {
    /// The room was open and is now archived.
    Archived,
    /// The room was already archived and stays archived.
    AlreadyArchived,
    /// The room was archived, and a pull request for its ticket has since been
    /// opened, so it is open again.
    Reopened,
    /// The room stays open.
    Open,
}

/// Every ticket room's merge verdict, for this router process. Outlives rooms
/// and connections, like the turn runs and allowances, so a room that empties
/// and re-forms is still archived.
#[derive(Default)]
pub struct Verdicts {
    entries: HashMap<RoomId, Entry>,
}

impl Verdicts {
    /// Whether `room` is archived. The only thing the ingest path reads.
    pub fn is_archived(&self, room: &RoomId) -> bool {
        self.entries
            .get(room)
            .is_some_and(|entry| matches!(entry.known, Known::Archived { .. }))
    }

    /// Whether a verdict for `room` is still current at `now`, so no check is
    /// needed.
    pub fn is_settled(&self, room: &RoomId, now: Instant) -> bool {
        match self.entries.get(room).map(|entry| entry.known) {
            Some(
                Known::Archived { recheck_at: until }
                | Known::NotMergedUntil(until)
                | Known::UnverifiableUntil(until),
            ) => now < until,
            None => false,
        }
    }

    /// Records the verdict of a check that started at `now`.
    ///
    /// An archived room stays archived whatever a later check says, with one
    /// exception: a check that started after the archive and found a pull
    /// request for the ticket **open** reopens it — part two of a stacked
    /// ticket, opened after part one merged. Nothing else reopens a room: not
    /// "no merged pull request found", which is also what a merge that has
    /// scrolled out of the listing looks like, and not an unverifiable check.
    /// So a repeated archive is a no-op, and a stale answer racing a fresh one
    /// cannot flip the room either way.
    pub fn record(&mut self, room: &RoomId, verdict: MergeVerdict, now: Instant) -> Recorded {
        if let Some(entry) = self.entries.get_mut(room)
            && matches!(entry.known, Known::Archived { .. })
        {
            if verdict == MergeVerdict::InProgress && now > entry.recorded_at {
                return match now.checked_add(NOT_MERGED_TTL) {
                    Some(until) => {
                        entry.known = Known::NotMergedUntil(until);
                        entry.recorded_at = now;
                        Recorded::Reopened
                    }
                    // Unrepresentable (rule 40): stay archived, and ask again
                    // on the next post.
                    None => Recorded::AlreadyArchived,
                };
            }
            if let Some(recheck_at) = now.checked_add(ARCHIVED_RECHECK) {
                entry.known = Known::Archived { recheck_at };
            }
            return Recorded::AlreadyArchived;
        }
        // An expiry that cannot be represented is not remembered at all: the
        // next post asks again, which is the direction that cannot leave a
        // stale answer standing (rule 40).
        let (known, recorded) = match verdict {
            MergeVerdict::Merged => match now.checked_add(ARCHIVED_RECHECK) {
                Some(recheck_at) => (Known::Archived { recheck_at }, Recorded::Archived),
                None => return Recorded::Open,
            },
            MergeVerdict::InProgress | MergeVerdict::NotMerged => {
                match now.checked_add(NOT_MERGED_TTL) {
                    Some(until) => (Known::NotMergedUntil(until), Recorded::Open),
                    None => return Recorded::Open,
                }
            }
            MergeVerdict::Unverifiable => match now.checked_add(UNVERIFIABLE_RETRY) {
                Some(until) => (Known::UnverifiableUntil(until), Recorded::Open),
                None => return Recorded::Open,
            },
        };
        if !self.entries.contains_key(room) && self.entries.len() >= MAX_VERDICTS {
            self.make_room(now);
        }
        self.entries.insert(
            room.clone(),
            Entry {
                known,
                recorded_at: now,
            },
        );
        recorded
    }

    /// Frees one slot: every expired open verdict if there is one, otherwise
    /// the oldest entry of any kind.
    ///
    /// Evicting an archived room forgets the archive. The next post re-checks,
    /// and finds the same merge only while it is still among the repo's
    /// `github::RECENT_PULLS` most recently updated pull requests; past that
    /// window the room reads as not merged and reopens. That is the fail-open
    /// direction, and the table is sized so it takes over a thousand ticket
    /// rooms to reach.
    fn make_room(&mut self, now: Instant) {
        self.entries.retain(|_, entry| match entry.known {
            Known::Archived { .. } => true,
            Known::NotMergedUntil(until) | Known::UnverifiableUntil(until) => now < until,
        });
        if self.entries.len() < MAX_VERDICTS {
            return;
        }
        let oldest = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.recorded_at)
            .map(|(room, _)| room.clone());
        if let Some(oldest) = oldest {
            self.entries.remove(&oldest);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use sealmesh_chat_wire::Scope;
    use sealmesh_wire::ProjectId;

    use super::*;
    use crate::auth::{Denied, PullsUnverifiable};

    fn ticket(text: &str) -> TicketId {
        TicketId::try_from(text).unwrap()
    }

    fn repo(slug: &str) -> RepoSlug {
        RepoSlug::try_from(slug).unwrap()
    }

    fn token(value: &str) -> SecretToken {
        SecretToken::new(value.to_owned())
    }

    fn pull(head: &str, state: PullState) -> PullSummary {
        PullSummary {
            head_ref: head.to_owned(),
            into_default_branch: true,
            state,
        }
    }

    fn pull_into(head: &str, base_is_default: bool, state: PullState) -> PullSummary {
        PullSummary {
            head_ref: head.to_owned(),
            into_default_branch: base_is_default,
            state,
        }
    }

    fn room(number: u32) -> RoomId {
        RoomId::new(
            ProjectId::try_from("sealmesh").unwrap(),
            Scope::Ticket(TicketId::new("XYZ", number).unwrap()),
        )
    }

    /// Answers each repo from a table, counts calls, and records which token
    /// asked about which repo.
    struct Table {
        pulls: Vec<(&'static str, Result<Vec<PullSummary>, PullsUnverifiable>)>,
        asked: Mutex<Vec<(String, String)>>,
        calls: AtomicUsize,
    }

    impl Table {
        fn new(pulls: Vec<(&'static str, Result<Vec<PullSummary>, PullsUnverifiable>)>) -> Self {
            Self {
                pulls,
                asked: Mutex::new(Vec::new()),
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl Directory for Table {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn recent_pulls(
            &self,
            token: &SecretToken,
            repo: &RepoSlug,
        ) -> Result<Vec<PullSummary>, PullsUnverifiable> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.asked
                .lock()
                .unwrap()
                .push((token.expose().to_owned(), repo.as_str().to_owned()));
            self.pulls
                .iter()
                .find(|(slug, _)| *slug == repo.as_str())
                .map_or(Ok(Vec::new()), |(_, answer)| answer.clone())
        }
    }

    #[test]
    fn a_branch_names_a_ticket_only_as_a_whole_id() {
        let jux62 = ticket("XYZ-62");
        for named in [
            "feature/xyz-62-archive-a-ticket-room",
            "feature/xyz-62-featserver-archive-a-ticket-room-when-its-pr-merges",
            "XYZ-62",
            "fix/XYZ-62",
            "xyz-62_followup",
            "sandbank/feature/xyz-62-x",
        ] {
            assert!(names_ticket(named, &jux62), "{named}");
        }
        for other in [
            "feature/xyz-620-something",
            "feature/axyz-62-x",
            "feature/xyz-6-x",
            "feature/jux62-x",
            "feature/xyz-62a",
            "feature/fix-xyz-62",
            "main",
            "",
        ] {
            assert!(!names_ticket(other, &jux62), "{other}");
        }
    }

    #[test]
    fn only_the_first_ticket_in_a_branch_counts() {
        // Linear appends the ticket's slugified title, and a title can name
        // another ticket. The first ticket-shaped token is the branch's own.
        assert!(!names_ticket("xyz-620/then/xyz-62", &ticket("XYZ-62")));
        assert!(!names_ticket(
            "feature/xyz-63-follow-up-on-xyz-62-retention",
            &ticket("XYZ-62")
        ));
        assert!(names_ticket(
            "feature/xyz-63-follow-up-on-xyz-62-retention",
            &ticket("XYZ-63")
        ));
    }

    #[tokio::test]
    async fn another_tickets_merge_whose_title_names_this_ticket_does_not_archive_it() {
        // XYZ-63, titled "Follow up on XYZ-62 retention", merges while
        // XYZ-62's agents are still coordinating with no pull request yet.
        let directory = Table::new(vec![(
            "example-org/sealmesh",
            Ok(vec![pull(
                "feature/xyz-63-follow-up-on-xyz-62-retention",
                PullState::Merged,
            )]),
        )]);
        let verdict = check_merged(
            &directory,
            &token("ghp_atlas"),
            &[repo("example-org/sealmesh")],
            &ticket("XYZ-62"),
            CHECK_BUDGET,
        )
        .await;
        assert_eq!(verdict, MergeVerdict::NotMerged);
    }

    #[tokio::test]
    async fn a_merge_into_a_branch_other_than_the_default_is_not_the_work_landing() {
        // Part two of a stack, merged into part one's branch. Part one is not
        // in the listing, so the only merge there is is the one that does
        // not count.
        let directory = Table::new(vec![(
            "example-org/sealmesh",
            Ok(vec![pull_into(
                "feature/xyz-62-part-2",
                false,
                PullState::Merged,
            )]),
        )]);
        let verdict = check_merged(
            &directory,
            &token("ghp_atlas"),
            &[repo("example-org/sealmesh")],
            &ticket("XYZ-62"),
            CHECK_BUDGET,
        )
        .await;
        assert_eq!(verdict, MergeVerdict::NotMerged);

        // And once part one lands on the default branch, it does.
        let directory = Table::new(vec![(
            "example-org/sealmesh",
            Ok(vec![
                pull_into("feature/xyz-62-part-2", false, PullState::Merged),
                pull_into("feature/xyz-62-part-1", true, PullState::Merged),
            ]),
        )]);
        let verdict = check_merged(
            &directory,
            &token("ghp_atlas"),
            &[repo("example-org/sealmesh")],
            &ticket("XYZ-62"),
            CHECK_BUDGET,
        )
        .await;
        assert_eq!(verdict, MergeVerdict::Merged);
    }

    #[tokio::test]
    async fn a_merged_pull_request_with_none_open_is_merged() {
        let directory = Table::new(vec![(
            "example-org/sealmesh",
            Ok(vec![
                pull("feature/xyz-62-archive", PullState::Merged),
                pull("feature/xyz-61-persist", PullState::Open),
            ]),
        )]);
        let verdict = check_merged(
            &directory,
            &token("ghp_atlas"),
            &[repo("example-org/sealmesh")],
            &ticket("XYZ-62"),
            CHECK_BUDGET,
        )
        .await;
        assert_eq!(verdict, MergeVerdict::Merged);
    }

    #[tokio::test]
    async fn an_open_pull_request_for_the_ticket_in_any_repo_keeps_it_open() {
        let directory = Table::new(vec![
            (
                "example-org/sealmesh",
                Ok(vec![pull("feature/xyz-62-part-1", PullState::Merged)]),
            ),
            (
                "example-org/example-tools",
                Ok(vec![pull("feature/xyz-62-part-2", PullState::Open)]),
            ),
        ]);
        let verdict = check_merged(
            &directory,
            &token("ghp_atlas"),
            &[
                repo("example-org/sealmesh"),
                repo("example-org/example-tools"),
            ],
            &ticket("XYZ-62"),
            CHECK_BUDGET,
        )
        .await;
        assert_eq!(verdict, MergeVerdict::InProgress);
    }

    #[tokio::test]
    async fn no_merged_pull_request_is_not_merged() {
        let directory = Table::new(vec![(
            "example-org/sealmesh",
            Ok(vec![
                pull("feature/xyz-62-abandoned", PullState::Closed),
                pull("feature/xyz-620-other", PullState::Merged),
            ]),
        )]);
        let verdict = check_merged(
            &directory,
            &token("ghp_atlas"),
            &[repo("example-org/sealmesh")],
            &ticket("XYZ-62"),
            CHECK_BUDGET,
        )
        .await;
        assert_eq!(verdict, MergeVerdict::NotMerged);
    }

    #[tokio::test]
    async fn one_repo_that_cannot_be_listed_makes_the_whole_check_unverifiable() {
        // The merged one is in the repo that answered; the one that did not
        // could hold an open pull request for the same ticket.
        let directory = Table::new(vec![
            (
                "example-org/sealmesh",
                Ok(vec![pull("feature/xyz-62-archive", PullState::Merged)]),
            ),
            ("example-org/example-tools", Err(PullsUnverifiable)),
        ]);
        let verdict = check_merged(
            &directory,
            &token("ghp_atlas"),
            &[
                repo("example-org/sealmesh"),
                repo("example-org/example-tools"),
            ],
            &ticket("XYZ-62"),
            CHECK_BUDGET,
        )
        .await;
        assert_eq!(verdict, MergeVerdict::Unverifiable);
    }

    #[tokio::test]
    async fn more_repos_than_the_cap_asks_github_nothing_and_is_unverifiable() {
        let directory = Table::new(Vec::new());
        let repos: Vec<RepoSlug> = (0..=MAX_REPOS_PER_CHECK)
            .map(|n| repo(&format!("example-org/repo-{n}")))
            .collect();
        let verdict = check_merged(
            &directory,
            &token("ghp_atlas"),
            &repos,
            &ticket("XYZ-62"),
            CHECK_BUDGET,
        )
        .await;
        assert_eq!(verdict, MergeVerdict::Unverifiable);
        assert_eq!(directory.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn at_the_cap_every_repo_is_asked_once() {
        let directory = Table::new(Vec::new());
        let repos: Vec<RepoSlug> = (0..MAX_REPOS_PER_CHECK)
            .map(|n| repo(&format!("example-org/repo-{n}")))
            .collect();
        let _ = check_merged(
            &directory,
            &token("ghp_atlas"),
            &repos,
            &ticket("XYZ-62"),
            CHECK_BUDGET,
        )
        .await;
        assert_eq!(directory.calls.load(Ordering::SeqCst), MAX_REPOS_PER_CHECK);
    }

    #[tokio::test]
    async fn the_check_asks_with_the_token_it_was_given_and_nothing_else() {
        let directory = Table::new(Vec::new());
        let _ = check_merged(
            &directory,
            &token("ghp_the_poster"),
            &[repo("example-org/sealmesh")],
            &ticket("XYZ-62"),
            CHECK_BUDGET,
        )
        .await;
        assert_eq!(
            *directory.asked.lock().unwrap(),
            vec![(
                "ghp_the_poster".to_owned(),
                "example-org/sealmesh".to_owned()
            )]
        );
    }

    /// Never answers.
    struct Hangs;

    impl Directory for Hangs {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn recent_pulls(
            &self,
            _token: &SecretToken,
            _repo: &RepoSlug,
        ) -> Result<Vec<PullSummary>, PullsUnverifiable> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn a_check_past_its_deadline_is_unverifiable_not_a_no() {
        let verdict = check_merged(
            &Hangs,
            &token("ghp_atlas"),
            &[repo("example-org/sealmesh")],
            &ticket("XYZ-62"),
            Duration::from_millis(20),
        )
        .await;
        assert_eq!(verdict, MergeVerdict::Unverifiable);
    }

    #[test]
    fn a_merged_verdict_archives_once_and_stays_archived() {
        let mut verdicts = Verdicts::default();
        let now = Instant::now();
        assert_eq!(
            verdicts.record(&room(62), MergeVerdict::Merged, now),
            Recorded::Archived
        );
        assert!(verdicts.is_archived(&room(62)));
        // Idempotent: a repeated signal is not an error and changes nothing.
        assert_eq!(
            verdicts.record(&room(62), MergeVerdict::Merged, now),
            Recorded::AlreadyArchived
        );
        // Settled until its re-check falls due.
        let before = now.checked_add(ARCHIVED_RECHECK / 2).unwrap();
        assert!(verdicts.is_settled(&room(62), before));
        let due = now.checked_add(ARCHIVED_RECHECK).unwrap();
        assert!(!verdicts.is_settled(&room(62), due));
        // A re-check that finds no merged pull request — a merge scrolled
        // out of the listing looks exactly like that — or cannot be made
        // does not reopen it.
        for later in [MergeVerdict::NotMerged, MergeVerdict::Unverifiable] {
            assert_eq!(
                verdicts.record(&room(62), later, due),
                Recorded::AlreadyArchived
            );
            assert!(verdicts.is_archived(&room(62)));
        }
        // And each re-check pushes the next one out.
        assert!(verdicts.is_settled(&room(62), due));
    }

    #[test]
    fn a_newly_opened_pull_request_for_the_ticket_reopens_an_archived_room() {
        // Part two of a stacked ticket, opened after part one merged.
        let mut verdicts = Verdicts::default();
        let merged_at = Instant::now();
        verdicts.record(&room(62), MergeVerdict::Merged, merged_at);
        let later = merged_at.checked_add(ARCHIVED_RECHECK).unwrap();
        assert_eq!(
            verdicts.record(&room(62), MergeVerdict::InProgress, later),
            Recorded::Reopened
        );
        assert!(!verdicts.is_archived(&room(62)));
        assert!(verdicts.is_settled(&room(62), later));
        // When part two merges, it archives again.
        let after = later.checked_add(NOT_MERGED_TTL).unwrap();
        assert_eq!(
            verdicts.record(&room(62), MergeVerdict::Merged, after),
            Recorded::Archived
        );
    }

    #[test]
    fn a_stale_open_answer_cannot_reopen_a_room_a_newer_check_archived() {
        // Two checks race: the one that started first saw the pull request
        // still open, the one that started later saw it merged and recorded
        // first. The older answer must not undo the newer.
        let mut verdicts = Verdicts::default();
        let older = Instant::now();
        let newer = older.checked_add(Duration::from_millis(5)).unwrap();
        verdicts.record(&room(62), MergeVerdict::Merged, newer);
        assert_eq!(
            verdicts.record(&room(62), MergeVerdict::InProgress, older),
            Recorded::AlreadyArchived
        );
        assert!(verdicts.is_archived(&room(62)));
    }

    #[test]
    fn an_unverifiable_verdict_never_archives_and_is_retried_sooner_than_a_no() {
        let mut verdicts = Verdicts::default();
        let now = Instant::now();
        assert_eq!(
            verdicts.record(&room(1), MergeVerdict::Unverifiable, now),
            Recorded::Open
        );
        assert_eq!(
            verdicts.record(&room(2), MergeVerdict::NotMerged, now),
            Recorded::Open
        );
        assert!(!verdicts.is_archived(&room(1)));
        assert!(verdicts.is_settled(&room(1), now));
        let retry = now.checked_add(UNVERIFIABLE_RETRY).unwrap();
        assert!(!verdicts.is_settled(&room(1), retry), "unverifiable stuck");
        assert!(verdicts.is_settled(&room(2), retry));
        let expired = now.checked_add(NOT_MERGED_TTL).unwrap();
        assert!(!verdicts.is_settled(&room(2), expired), "not merged stuck");
        const { assert!(UNVERIFIABLE_RETRY.as_secs() < NOT_MERGED_TTL.as_secs()) };
    }

    #[test]
    fn the_verdict_table_is_capped() {
        let mut verdicts = Verdicts::default();
        let now = Instant::now();
        let total = u32::try_from(MAX_VERDICTS).unwrap() + 10;
        for number in 1..=total {
            verdicts.record(&room(number), MergeVerdict::Merged, now);
        }
        assert_eq!(verdicts.len(), MAX_VERDICTS);
    }

    #[test]
    fn a_full_table_frees_expired_entries_before_evicting_a_live_one() {
        let mut verdicts = Verdicts::default();
        let start = Instant::now();
        verdicts.record(&room(1), MergeVerdict::Merged, start);
        let max = u32::try_from(MAX_VERDICTS).unwrap();
        for number in 2..=max {
            verdicts.record(&room(number), MergeVerdict::NotMerged, start);
        }
        let later = start.checked_add(NOT_MERGED_TTL).unwrap();
        verdicts.record(&room(max + 1), MergeVerdict::NotMerged, later);
        assert!(
            verdicts.is_archived(&room(1)),
            "evicted a live verdict while expired ones were resident"
        );
        assert_eq!(verdicts.len(), 2);
    }
}
