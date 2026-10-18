//! Reading rooms from a daemon connection: history backfill and the room list
//!.
//!
//! Two requests, both answered on the daemon's own socket, both run as a
//! pinned future in that connection's `select!` so a slow store or a slow
//! GitHub parks only their own branch — never the one that answers `Query`.
//! Neither takes the process-wide ingest lock: a read orders against nothing,
//! and waiting on the store under that lock would stall posting in every room.
//!
//! # Backfill
//!
//! Only a room the connection holds is read. Membership already passed the
//! join check and is re-checked every period, so a backfill asks GitHub
//! nothing. Anything else is one refusal, decided before the store is asked,
//! so a room that is someone else's, one with history and no members, and one
//! that never existed are the same answer in the same time (rule 25).
//!
//! The reply is one frame. `history::MAX_PAGE_BYTES` is measured on stored
//! bodies, but the frame cap is measured on JSON, where a control character
//! becomes a six-byte escape — a page of 64 maximum-size bodies is about
//! 1.5 MiB on the wire, three times `MAX_FRAME_BYTES`. So the read stops at the
//! first message that would push the encoded reply past the frame cap, and
//! `next` says where to resume. [`frame_fits`] is that check, measured on the
//! exact encoding the socket sends.
//!
//! # The room list
//!
//! Live rooms only: rooms the registry holds, which means rooms with members.
//! Each is put through `room_access::authorise_join` — the join's own decision,
//! not a copy of it — with the connection's own token, through the access
//! cache. A room it refuses, cannot finish, or cannot afford to ask about is
//! left out, which fails closed and says nothing about why.
//!
//! What one listing may spend is bounded three ways:
//!
//! - At most [`MAX_LIST_CHECKS`] access checks that reach GitHub. A cache hit is
//!   free.
//! - Each of those is charged to the requesting host's allowance in
//!   `check_budget`, the same allowance a reader's fan-out draws on. The
//!   resource being protected is that person's GitHub rate limit, and their
//!   daemon and their `who` spend it with the same credential, so one
//!   allowance per host is the honest key (rule 12: the host is the one the
//!   credential resolved to at the handshake).
//! - [`LIST_BUDGET`] of wall-clock for the whole listing.
//!
//! And the reply carries at most `MAX_LISTED_ROOMS`, a cap the wire type
//! enforces on both ends.

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use sealmesh_chat_wire::RoomId;
use sealmesh_proto::{
    BackfillRefusal, HistoryEntry, MAX_FRAME_BYTES, MAX_LISTED_ROOMS, RoomSeq, SecretToken,
    ServerFrame,
};
use sealmesh_wire::{HostId, RepoSlug};

use crate::{
    auth::{Denied, Directory, PullSummary, PullsUnverifiable, RepoAccess},
    check_budget::CheckBudgets,
    history::{self, HistoryError, Record, RoomHistory},
    projects::ProjectMap,
    room_access::authorise_join,
    rooms,
};

/// Most access checks one room listing may send to GitHub. Cache hits are not
/// counted.
///
/// A listing is a convenience, not an authorisation anyone is waiting on, so
/// it gets the same per-request ceiling as one fan-out
/// (`query::MAX_REPOS_PER_QUERY`), and never more than half the burst of the
/// allowance it is charged to.
pub const MAX_LIST_CHECKS: usize = 32;

/// Wall-clock budget for one whole listing: the same ten seconds one join's
/// check gets. A room not decided by then is left out.
pub const LIST_BUDGET: Duration = rooms::JOIN_BUDGET;

/// A stored message as a backfill reply carries it.
fn entry(record: &Record) -> HistoryEntry {
    HistoryEntry {
        seq: record.seq,
        from: record.from.clone(),
        kind: record.kind,
        body: record.body.clone(),
    }
}

fn encoded_len<T: serde::Serialize>(value: &T) -> Option<usize> {
    serde_json::to_vec(value).ok().map(|bytes| bytes.len())
}

/// A running check that a backfill reply for `room` still fits one frame.
///
/// Starts from the reply's envelope with both cursors at their widest, so the
/// real cursors can only make it smaller, then adds each entry's exact encoded
/// length and, after the first, its separating comma — so with cursors at
/// full width the count is the reply's exact length, and a cut is as tight
/// as the next message allows. Measured with `serde_json`, which is what
/// `http::send` encodes with, so the escaping counted is the escaping sent.
/// Anything that cannot be measured does not fit.
pub(crate) fn frame_fits(room: &RoomId) -> impl FnMut(&Record) -> bool + Send + use<> {
    let envelope = encoded_len(&ServerFrame::Backfill {
        room: room.clone(),
        after: Some(RoomSeq(u64::MAX)),
        messages: Vec::new(),
        next: Some(RoomSeq(u64::MAX)),
    });
    let mut used = envelope;
    let mut first = true;
    move |record: &Record| {
        let (Some(so_far), Some(len)) = (used, encoded_len(&entry(record))) else {
            return false;
        };
        // Entries after the first are preceded by a comma.
        let separator = usize::from(!first);
        let Some(total) = so_far
            .checked_add(len)
            .and_then(|n| n.checked_add(separator))
        else {
            return false;
        };
        if total > MAX_FRAME_BYTES {
            return false;
        }
        used = Some(total);
        first = false;
        true
    }
}

/// Reads one frame's worth of `room`'s history after `after`, within `budget`.
///
/// `room` must be the registry's key for a room the caller has already
/// confirmed this connection holds; this function checks nothing about who is
/// asking. Every failure — a slow store, an unreachable one, an item it will
/// not trust — is the one category `Unavailable` to the peer, and the
/// `HistoryError` for the log, which carries no stored bytes (rules 14, 26).
pub(crate) async fn read_reply(
    store: &dyn RoomHistory,
    room: &RoomId,
    after: Option<RoomSeq>,
    budget: Duration,
) -> Result<ServerFrame, HistoryError> {
    let read = history::backfill_until(store, room, after.map(|seq| seq.0), frame_fits(room));
    let page = tokio::time::timeout(budget, read)
        .await
        .map_err(|_elapsed| HistoryError::Unavailable)??;
    Ok(ServerFrame::Backfill {
        room: room.clone(),
        after,
        messages: page.messages.iter().map(entry).collect(),
        next: page.next.map(RoomSeq),
    })
}

/// The refusal a backfill gets, carrying the room exactly as the daemon sent
/// it — never the registry's spelling, which would tell a non-member whether
/// the repo it named is mapped.
pub(crate) fn refuse(room: RoomId, category: BackfillRefusal) -> ServerFrame {
    ServerFrame::BackfillRefused { room, category }
}

/// What one listing spent, for its log line.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ListSpend {
    /// Checks sent to GitHub.
    pub made: usize,
    /// Checks not sent because [`MAX_LIST_CHECKS`] was reached or the host's
    /// allowance was spent. Each one left its room out.
    pub refused: usize,
    /// Whether [`LIST_BUDGET`] ran out before every candidate was decided.
    pub timed_out: bool,
}

/// A [`Directory`] that answers `can_read` from the cache for free and charges
/// every other check to one host, refusing it as `Unverifiable` once either
/// the per-listing ceiling or the host's allowance is spent.
///
/// `Unverifiable` is the answer `authorise_join` already refuses on, so an
/// unaffordable check leaves its room out through the same decision a join
/// uses rather than through a second rule beside it.
struct Charged<'a, D> {
    inner: &'a D,
    budgets: &'a CheckBudgets,
    payer: &'a HostId,
    made: AtomicUsize,
    refused: AtomicUsize,
}

impl<D: Directory> Charged<'_, D> {
    /// Takes one slot under [`MAX_LIST_CHECKS`], then one check from the
    /// payer's allowance. The ceiling is taken first, so a check the listing
    /// could not have made anyway costs the allowance nothing.
    fn afford(&self) -> bool {
        let slot = self
            .made
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |made| {
                (made < MAX_LIST_CHECKS).then(|| made.saturating_add(1))
            })
            .is_ok();
        if slot && self.budgets.try_charge(self.payer) {
            return true;
        }
        if slot {
            // The slot was taken for a check that was not made.
            self.made.fetch_sub(1, Ordering::SeqCst);
        }
        self.refused.fetch_add(1, Ordering::SeqCst);
        false
    }
}

impl<D: Directory> Directory for Charged<'_, D> {
    async fn login(&self, token: &SecretToken) -> Result<String, Denied> {
        self.inner.login(token).await
    }

    async fn is_member(&self, token: &SecretToken) -> Result<bool, Denied> {
        self.inner.is_member(token).await
    }

    async fn can_read(&self, token: &SecretToken, repo: &RepoSlug) -> RepoAccess {
        if let Some(known) = self.inner.known_access(token, repo) {
            return known;
        }
        if !self.afford() {
            return RepoAccess::Unverifiable;
        }
        self.inner.can_read(token, repo).await
    }

    fn known_access(&self, token: &SecretToken, repo: &RepoSlug) -> Option<RepoAccess> {
        self.inner.known_access(token, repo)
    }

    async fn recent_pulls(
        &self,
        token: &SecretToken,
        repo: &RepoSlug,
    ) -> Result<Vec<PullSummary>, PullsUnverifiable> {
        self.inner.recent_pulls(token, repo).await
    }
}

/// The rooms out of `candidates`, in their order, that `token` passes the
/// join check for — at most `MAX_LISTED_ROOMS` of them, with the checks that
/// reach GitHub bounded and charged to `payer` as the module docs describe.
///
/// `token` is the listing connection's own, borrowed for the call and handed
/// only to the directory.
pub(crate) async fn readable_rooms<D: Directory>(
    candidates: Vec<RoomId>,
    directory: &D,
    projects: &ProjectMap,
    token: &SecretToken,
    budgets: &CheckBudgets,
    payer: &HostId,
    budget: Duration,
) -> (Vec<RoomId>, ListSpend) {
    let now = tokio::time::Instant::now();
    // A deadline that cannot be represented is treated as already passed:
    // nothing is listed, the fail-closed side.
    let deadline = now.checked_add(budget).unwrap_or(now);
    let charged = Charged {
        inner: directory,
        budgets,
        payer,
        made: AtomicUsize::new(0),
        refused: AtomicUsize::new(0),
    };

    let mut listed = Vec::new();
    let mut timed_out = false;
    for room in candidates {
        if listed.len() >= MAX_LISTED_ROOMS {
            break;
        }
        match tokio::time::timeout_at(deadline, authorise_join(&charged, projects, token, &room))
            .await
        {
            Ok(Ok(())) => listed.push(room),
            Ok(Err(_denied)) => {}
            Err(_elapsed) => {
                timed_out = true;
                break;
            }
        }
    }
    let spend = ListSpend {
        made: charged.made.load(Ordering::SeqCst),
        refused: charged.refused.load(Ordering::SeqCst),
        timed_out,
    };
    (listed, spend)
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

    use sealmesh_chat_wire::{Body, MAX_BODY_CHARS, MessageKind, Scope};
    use sealmesh_proto::decode;
    use sealmesh_wire::ProjectId;

    use super::*;
    use crate::{check_budget::ChecksPerHour, history::Fake};

    fn widest_room() -> RoomId {
        let longest_repo = format!("{}/{}", "a".repeat(100), "b".repeat(100));
        RoomId::new(
            ProjectId::try_from("p".repeat(32).as_str()).unwrap(),
            Scope::Repo(RepoSlug::try_from(longest_repo.as_str()).unwrap()),
        )
    }

    fn project_room(name: &str) -> RoomId {
        RoomId::new(ProjectId::try_from(name).unwrap(), Scope::Project)
    }

    fn record(room: &RoomId, seq: u64, from: &HostId, body: Body) -> Record {
        Record {
            room: room.clone(),
            seq: RoomSeq(seq),
            from: from.clone(),
            kind: MessageKind::Handoff,
            body,
            // A minute ago, not a fixed epoch second: XYZ-108 makes the read
            // path filter on `expires_at`, computed from `posted_at`, against
            // the real clock. A fixed historical timestamp eventually — and
            // for a worktree checked out well after this was written,
            // immediately — reads as already expired and vanishes from every
            // page (worker contract rule 4: a fixture must not depend on when
            // it happens to run).
            posted_at: crate::history::now_epoch()
                .unwrap_or(1_700_000_000)
                .saturating_sub(60),
        }
    }

    /// Every body at the character cap, every character four UTF-8 bytes, the
    /// widest host, and sequence numbers at the top of the range: the most
    /// bytes a stored message can become on the wire. A control character
    /// would escape to six, but a `Body` replaces one before it is stored
    ///, so four is the widest a body the router sends can be.
    fn worst_case_store(room: &RoomId, count: u64) -> Fake {
        let store = Fake::new();
        let worst = Body::new("😀".repeat(MAX_BODY_CHARS)).unwrap();
        let widest_host = HostId::try_from("h".repeat(32).as_str()).unwrap();
        let first = u64::MAX - count;
        for offset in 1..=count {
            store.insert(record(room, first + offset, &widest_host, worst.clone()));
        }
        store
    }

    fn seqs(frame: &ServerFrame) -> (Vec<u64>, Option<RoomSeq>) {
        match frame {
            ServerFrame::Backfill { messages, next, .. } => {
                (messages.iter().map(|m| m.seq.0).collect(), *next)
            }
            other => panic!("expected a backfill, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_page_of_worst_case_messages_is_cut_to_fit_one_frame_and_resumes_where_it_stopped() {
        let room = widest_room();
        let count = u64::try_from(history::MAX_PAGE_ITEMS).unwrap();
        let store = worst_case_store(&room, count);

        let mut after = None;
        let mut read = Vec::new();
        let mut replies = 0;
        loop {
            let reply = read_reply(&store, &room, after, Duration::from_secs(5))
                .await
                .unwrap();
            let encoded = serde_json::to_vec(&reply).unwrap();
            assert!(
                encoded.len() <= MAX_FRAME_BYTES,
                "a backfill reply is {} bytes, over MAX_FRAME_BYTES ({MAX_FRAME_BYTES})",
                encoded.len()
            );
            // The peer's own decoder accepts it, cap and all.
            assert_eq!(decode::<ServerFrame>(&encoded).unwrap(), reply);

            let (got, next) = seqs(&reply);
            assert!(!got.is_empty(), "a reply made no progress");
            // Full enough that the frame cap, not something else, cut it.
            assert!(
                next.is_none() || encoded.len() > MAX_FRAME_BYTES - 30_000,
                "a reply stopped at {} bytes with more to come",
                encoded.len()
            );
            read.extend(got);
            replies += 1;
            match next {
                Some(next) => after = Some(next),
                None => break,
            }
        }

        // The fixture really is the worst case: one store page is two
        // frames' worth, so it could not have been sent whole.
        assert!(replies >= 2, "{replies} replies");
        let expected: Vec<u64> = (u64::MAX - count + 1..=u64::MAX).collect();
        assert_eq!(read, expected, "messages were lost, repeated or reordered");
    }

    /// Reads one reply from 64 stored copies of `body` in `room`, and checks
    /// the cut from both sides: the reply fits the frame cap, and when it
    /// stopped early, one more message would not have.
    async fn assert_cut_is_inside_the_cap_and_tight(room: &RoomId, body: &Body, case: &str) {
        let widest_host = HostId::try_from("h".repeat(32).as_str()).unwrap();
        let count = u64::try_from(history::MAX_PAGE_ITEMS).unwrap();
        // Wide cursors on both sides, as a real read deep into a room has.
        let after = Some(RoomSeq(u64::MAX - count - 1));
        let store = Fake::new();
        for seq in u64::MAX - count + 1..=u64::MAX {
            store.insert(record(room, seq, &widest_host, body.clone()));
        }

        let reply = read_reply(&store, room, after, Duration::from_secs(5))
            .await
            .unwrap();
        let encoded = serde_json::to_vec(&reply).unwrap();
        assert!(
            encoded.len() <= MAX_FRAME_BYTES,
            "{case}: a {}-byte reply, over MAX_FRAME_BYTES ({MAX_FRAME_BYTES})",
            encoded.len()
        );
        let (got, next) = seqs(&reply);
        assert!(!got.is_empty(), "{case}: a reply made no progress");
        if let Some(RoomSeq(next)) = next {
            let one_more =
                encoded_len(&entry(&record(room, next + 1, &widest_host, body.clone()))).unwrap();
            assert!(
                encoded.len() + one_more + 1 > MAX_FRAME_BYTES,
                "{case}: the reply stopped at {} bytes although one more {one_more}-byte \
                 message would have fit",
                encoded.len()
            );
        }
    }

    fn mixed_body(escaped: usize) -> Body {
        Body::new(format!(
            "{}{}",
            "\u{1}".repeat(escaped),
            "a".repeat(MAX_BODY_CHARS - escaped)
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn every_cut_lands_inside_the_frame_cap_and_as_close_to_it_as_one_more_message_allows() {
        // Rule 42 at the boundary. With identical worst-case bodies the cut
        // lands wherever it happens to, so an envelope left uncounted,
        // measured with narrow cursors, or separating commas left out can
        // each go unnoticed — and an over-cap reply is a frame the daemon's
        // transport refuses, a dropped connection, and the same cursor asked
        // for again forever.
        //
        // Varying how many characters of each body escape moves every
        // entry's length, and with it where the cut falls. A sweep samples
        // the cut; the test below puts it exactly on the cap.
        let room = widest_room();
        for escaped in (0..=MAX_BODY_CHARS).step_by(4) {
            assert_cut_is_inside_the_cap_and_tight(
                &room,
                &mixed_body(escaped),
                &format!("{escaped} escaped characters per body"),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn a_reply_exactly_at_the_cap_is_sent_whole_and_one_byte_over_loses_a_message() {
        // A sweep only samples where the cut falls, and a miscount of one
        // byte per entry — a separating comma left out — matters only when
        // the space left under the cap is smaller than the number of entries,
        // a window a sampled sweep can step over. So these replies are built
        // to be exactly `MAX_FRAME_BYTES` with `n` entries, and exactly one
        // byte over it: the first must carry all `n`, the second `n - 1`.
        // Envelope and entry lengths are measured with `serde_json`, and every
        // sequence number here is twenty digits, as wide as `u64::MAX`.
        let host = HostId::try_from("h".repeat(32).as_str()).unwrap();
        // Wide characters are four UTF-8 bytes each: the widest a body can
        // carry now that a control character never reaches one.
        let entry_len = |wide: usize, plain: usize| {
            encoded_len(&HistoryEntry {
                seq: RoomSeq(u64::MAX - 1),
                from: host.clone(),
                kind: MessageKind::Handoff,
                body: Body::new(format!("{}{}", "😀".repeat(wide), "a".repeat(plain))).unwrap(),
            })
            .unwrap()
        };
        // An entry is this long plus four bytes per wide character and one
        // per plain one.
        let bare = entry_len(0, 1) - 1;
        let room_of = |narrower: usize| {
            let repo = format!("{}/{}", "a".repeat(100), "b".repeat(100 - narrower));
            RoomId::new(
                ProjectId::try_from("p".repeat(32).as_str()).unwrap(),
                Scope::Repo(RepoSlug::try_from(repo.as_str()).unwrap()),
            )
        };
        let envelope = |room: &RoomId| {
            encoded_len(&ServerFrame::Backfill {
                room: room.clone(),
                after: Some(RoomSeq(u64::MAX - 1)),
                messages: Vec::new(),
                next: Some(RoomSeq(u64::MAX - 1)),
            })
            .unwrap()
        };

        let mut built = 0;
        // At four bytes a character an entry is at most about 16 KB, so a
        // reply of fewer than 33 entries cannot reach the cap.
        for n in [33usize, 40, 47, 63] {
            for over in [0usize, 1] {
                // n entries and n - 1 commas: E + n·L + n - 1 = cap + over.
                let Some((narrower, len)) = (0..=150).find_map(|narrower| {
                    let target = MAX_FRAME_BYTES + over + 1 - envelope(&room_of(narrower));
                    target.is_multiple_of(n).then(|| (narrower, target / n - 1))
                }) else {
                    continue;
                };
                let body_bytes = len - bare;
                let wide = (body_bytes / 4).min(MAX_BODY_CHARS - 1);
                let plain = body_bytes - 4 * wide;
                if wide + plain > MAX_BODY_CHARS || wide + plain == 0 {
                    continue;
                }
                assert_eq!(entry_len(wide, plain), len);

                let room = room_of(narrower);
                let body =
                    Body::new(format!("{}{}", "😀".repeat(wide), "a".repeat(plain))).unwrap();
                let store = Fake::new();
                let count = u64::try_from(history::MAX_PAGE_ITEMS).unwrap();
                for seq in u64::MAX - count + 1..=u64::MAX {
                    store.insert(record(&room, seq, &host, body.clone()));
                }
                let reply = read_reply(
                    &store,
                    &room,
                    Some(RoomSeq(u64::MAX - count - 1)),
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
                let encoded = serde_json::to_vec(&reply).unwrap();
                let (got, _) = seqs(&reply);
                let case = format!("{n} entries, {over} byte(s) over");
                assert!(
                    encoded.len() <= MAX_FRAME_BYTES,
                    "{case}: a {}-byte reply, over MAX_FRAME_BYTES ({MAX_FRAME_BYTES})",
                    encoded.len()
                );
                assert_eq!(got.len(), n - over, "{case}");
                if over == 0 {
                    assert_eq!(encoded.len(), MAX_FRAME_BYTES, "{case}");
                }
                built += 1;
            }
        }
        assert!(built >= 6, "only {built} boundary replies could be built");
    }

    #[tokio::test]
    async fn a_member_reads_history_in_order_across_store_pages() {
        let room = project_room("example-repo");
        let author = HostId::try_from("sandbank").unwrap();
        let store = Fake::new();
        // More than one backfill's worth of store pages.
        let total = u64::try_from(history::MAX_PAGES * 64 + 40).unwrap();
        for seq in 1..=total {
            store.insert(record(
                &room,
                seq,
                &author,
                Body::new(format!("m{seq}")).unwrap(),
            ));
        }

        let first = read_reply(&store, &room, None, Duration::from_secs(5))
            .await
            .unwrap();
        let (got, next) = seqs(&first);
        assert_eq!(got, (1..=1024).collect::<Vec<_>>());
        assert_eq!(next, Some(RoomSeq(1024)));

        let second = read_reply(&store, &room, next, Duration::from_secs(5))
            .await
            .unwrap();
        let (got, next) = seqs(&second);
        assert_eq!(got, (1025..=total).collect::<Vec<_>>());
        assert_eq!(next, None);
        match second {
            ServerFrame::Backfill { after, .. } => assert_eq!(after, Some(RoomSeq(1024))),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn an_unreadable_or_slow_store_is_one_category_and_carries_nothing_it_returned() {
        let room = project_room("example-repo");
        let store = Fake::new();
        store.fail_reads();
        assert_eq!(
            read_reply(&store, &room, None, Duration::from_secs(5)).await,
            Err(HistoryError::Unavailable)
        );

        let slow = Fake::new();
        slow.set_page_delay(Duration::from_secs(5));
        assert_eq!(
            read_reply(&slow, &room, None, Duration::from_millis(20)).await,
            Err(HistoryError::Unavailable)
        );
    }

    /// A directory that grants `readable` repos to one token, counts every
    /// call it answers, and records which token asked.
    struct Counting {
        readable: Vec<String>,
        unverifiable: Vec<String>,
        calls: AtomicUsize,
        tokens: Mutex<Vec<String>>,
    }

    impl Counting {
        fn new(readable: &[&str], unverifiable: &[&str]) -> Self {
            Self {
                readable: readable.iter().map(|r| (*r).to_owned()).collect(),
                unverifiable: unverifiable.iter().map(|r| (*r).to_owned()).collect(),
                calls: AtomicUsize::new(0),
                tokens: Mutex::new(Vec::new()),
            }
        }
    }

    impl Directory for Counting {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Err(Denied::NotAuthorised)
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(false)
        }
        async fn can_read(&self, token: &SecretToken, repo: &RepoSlug) -> RepoAccess {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.tokens.lock().unwrap().push(token.expose().to_owned());
            if self.unverifiable.iter().any(|r| r == repo.as_str()) {
                RepoAccess::Unverifiable
            } else if self.readable.iter().any(|r| r == repo.as_str()) {
                RepoAccess::Readable
            } else {
                RepoAccess::NotReadable
            }
        }
    }

    /// `count` projects, `p0`…, each with one repo `o/r<n>`.
    fn many_projects(count: usize) -> (ProjectMap, Vec<RoomId>, Vec<String>) {
        let entries: Vec<String> = (0..count)
            .map(|n| format!(r#"{{"id":"p{n}","repos":["o/r{n}"]}}"#))
            .collect();
        let map = ProjectMap::parse(&format!("[{}]", entries.join(","))).unwrap();
        let rooms = (0..count).map(|n| project_room(&format!("p{n}"))).collect();
        let repos = (0..count).map(|n| format!("o/r{n}")).collect();
        (map, rooms, repos)
    }

    fn token() -> SecretToken {
        SecretToken::new("ghp_daemon".to_owned())
    }

    fn payer() -> HostId {
        HostId::try_from("daemon").unwrap()
    }

    #[tokio::test]
    async fn a_listing_never_carries_more_than_the_room_cap() {
        // More readable rooms than the cap, and a check budget wide enough
        // that only the room cap can stop it: every check is a cache hit.
        struct AllKnown;
        impl Directory for AllKnown {
            async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
                Err(Denied::NotAuthorised)
            }
            async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
                Ok(false)
            }
            fn known_access(&self, _token: &SecretToken, _repo: &RepoSlug) -> Option<RepoAccess> {
                Some(RepoAccess::Readable)
            }
        }
        let (map, rooms, _) = many_projects(MAX_LISTED_ROOMS + 10);
        let (listed, spend) = readable_rooms(
            rooms.clone(),
            &AllKnown,
            &map,
            &token(),
            &CheckBudgets::default(),
            &payer(),
            LIST_BUDGET,
        )
        .await;
        assert_eq!(listed.len(), MAX_LISTED_ROOMS);
        assert_eq!(listed, rooms[..MAX_LISTED_ROOMS].to_vec());
        assert_eq!(spend.made, 0, "a cache hit was charged");
    }

    #[tokio::test]
    async fn a_listing_sends_github_no_more_than_its_check_ceiling() {
        let (map, rooms, repos) = many_projects(MAX_LISTED_ROOMS + 10);
        let repo_refs: Vec<&str> = repos.iter().map(String::as_str).collect();
        let directory = Counting::new(&repo_refs, &[]);
        let (listed, spend) = readable_rooms(
            rooms,
            &directory,
            &map,
            &token(),
            &CheckBudgets::new(ChecksPerHour::MAX),
            &payer(),
            LIST_BUDGET,
        )
        .await;
        assert_eq!(directory.calls.load(Ordering::SeqCst), MAX_LIST_CHECKS);
        assert_eq!(listed.len(), MAX_LIST_CHECKS);
        assert_eq!(spend.made, MAX_LIST_CHECKS);
        assert_eq!(spend.refused, MAX_LISTED_ROOMS + 10 - MAX_LIST_CHECKS);
    }

    #[tokio::test]
    async fn a_listing_is_charged_to_the_hosts_allowance_and_stops_when_it_is_spent() {
        let (map, rooms, repos) = many_projects(20);
        let repo_refs: Vec<&str> = repos.iter().map(String::as_str).collect();
        let directory = Counting::new(&repo_refs, &[]);
        let budgets = CheckBudgets::new(ChecksPerHour::MIN);
        // Spend all but five of this host's burst elsewhere — on its `who`,
        // say, which draws on the same allowance.
        let burst = crate::check_budget::CHECK_BURST;
        for _ in 0..burst - 5 {
            assert!(budgets.try_charge(&payer()));
        }
        let (listed, spend) = readable_rooms(
            rooms,
            &directory,
            &map,
            &token(),
            &budgets,
            &payer(),
            LIST_BUDGET,
        )
        .await;
        assert_eq!(directory.calls.load(Ordering::SeqCst), 5);
        assert_eq!(listed.len(), 5);
        assert_eq!(spend.refused, 15);
        assert!(
            !budgets.try_charge(&payer()),
            "the listing did not spend it"
        );
    }

    #[tokio::test]
    async fn a_listing_leaves_out_every_room_the_join_check_would_refuse() {
        let map = ProjectMap::parse(
            r#"[
                {"id":"open","repos":["o/open"]},
                {"id":"half","repos":["o/open2","o/closed"]},
                {"id":"closed","repos":["o/closed2"]},
                {"id":"broken","repos":["o/broken"]}
            ]"#,
        )
        .unwrap();
        let directory = Counting::new(&["o/open", "o/open2"], &["o/broken"]);
        let open_repo = RoomId::new(
            ProjectId::try_from("open").unwrap(),
            Scope::Repo(RepoSlug::try_from("o/open").unwrap()),
        );
        let half_repo = RoomId::new(
            ProjectId::try_from("half").unwrap(),
            Scope::Repo(RepoSlug::try_from("o/open2").unwrap()),
        );
        let candidates = vec![
            project_room("open"),
            open_repo.clone(),
            // A project room needs every repo; one is unreadable.
            project_room("half"),
            // A repo room in that project needs only its own repo.
            half_repo.clone(),
            project_room("closed"),
            // GitHub could not be asked: left out, never listed.
            project_room("broken"),
            // Not in the map at all.
            project_room("ghost"),
        ];
        let (listed, _) = readable_rooms(
            candidates,
            &directory,
            &map,
            &token(),
            &CheckBudgets::default(),
            &payer(),
            LIST_BUDGET,
        )
        .await;
        assert_eq!(listed, vec![project_room("open"), open_repo, half_repo]);
        // Every check used the listing connection's own token.
        assert!(
            directory
                .tokens
                .lock()
                .unwrap()
                .iter()
                .all(|t| t == "ghp_daemon")
        );
    }

    #[tokio::test]
    async fn a_listing_that_runs_out_of_time_lists_only_what_it_decided() {
        struct Stuck;
        impl Directory for Stuck {
            async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
                Err(Denied::NotAuthorised)
            }
            async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
                Ok(false)
            }
            async fn can_read(&self, _token: &SecretToken, _repo: &RepoSlug) -> RepoAccess {
                std::future::pending().await
            }
        }
        let (map, rooms, _) = many_projects(3);
        let (listed, spend) = readable_rooms(
            rooms,
            &Stuck,
            &map,
            &token(),
            &CheckBudgets::default(),
            &payer(),
            Duration::from_millis(20),
        )
        .await;
        assert!(listed.is_empty());
        assert!(spend.timed_out);
    }

    #[test]
    fn the_listing_ceiling_stays_inside_one_burst_of_the_allowance_it_draws_on() {
        assert!(
            u64::try_from(MAX_LIST_CHECKS).unwrap() <= crate::check_budget::CHECK_BURST / 2,
            "one listing could spend more than half a host's burst"
        );
    }
}
