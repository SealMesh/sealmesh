//! A short-TTL cache over [`Directory::can_read`], so a fan-out or a room
//! join that asks the same caller about the same repo twice in quick
//! succession does not hit GitHub twice.
//!
//! `login` and `is_member` are not cached here — they run once per connection
//! at the handshake, not once per query, so the fan-out cost this module
//! exists to cut does not touch them.
//!
//! Three properties the ticket asked for, each load-bearing:
//!
//! - The cache key includes the **caller**, not just the repo. Keying on repo
//!   alone would serve one engineer's access answer to a different engineer's
//!   token, which is exactly the client-supplied-identity bug rule 12 exists
//!   to rule out — except here the "client" is another cache entry rather
//!   than a frame field. See `one_callers_cached_grant_is_never_served_to_another`.
//! - [`RepoAccess::Unverifiable`] is never cached, in either direction. A
//!   cached negative would turn one outage into a sticky denial for the whole
//!   TTL; a cached positive would turn one lucky race into a sticky grant.
//!   Both are worse than paying for GitHub on every call until the network
//!   answers again — see the module docs on `CachedAccess` for the type-level
//!   version of this rule.
//! - The TTL has a hard ceiling ([`MAX_TTL`]) enforced in the constructor, not
//!   only in whatever validates the configured value on the way in. A stale
//!   grant is an authorization decision that has outlived its evidence, and
//!   the ceiling is what bounds how stale "stale" can get regardless of what
//!   gets passed to `new`.

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::Mutex,
    time::{Duration, Instant},
};

use sealmesh_proto::SecretToken;
use sealmesh_wire::RepoSlug;
use sha2::{Digest as _, Sha256};

use crate::auth::{Denied, Directory, PullSummary, PullsUnverifiable, RepoAccess};

/// No configured TTL is honoured past this, no matter what set it. Five
/// minutes is long enough to absorb a fan-out and a room join asking about the
/// same repo back to back, and short enough that a revoked grant does not
/// outlive it by much.
pub const MAX_TTL: Duration = Duration::from_secs(300);

/// Hard cap on resident entries (rule 44: every remote-triggered read is
/// bounded in count as well as bytes). A query fans out to every attached
/// daemon and every repo a reader asks about adds at most one entry per
/// caller, so this is generous for a two-engineer org and still a real bound
/// if the caller set grows.
pub const MAX_ENTRIES: usize = 4096;

/// A one-way stand-in for "who is asking", built from the caller's own bearer
/// token by SHA-256.
///
/// This is deliberately not the token itself: a `SecretToken` must never be
/// stored (rules 6, 7 — no `Display`, a redacting `Debug`, and that posture
/// only holds if nothing else on the heap holds the plaintext either). A
/// digest of a high-entropy GitHub token is not meaningfully reversible, and
/// it is exactly as good a caller key as the token is a caller credential:
/// GitHub already trusts this same bearer value to resolve one identity, so
/// two different callers cannot collide on it any more than they could
/// authenticate as each other (barring a SHA-256 collision). This is the
/// "derive identity from the credential, never from anything asserted" rule
/// (12) applied to a cache key instead of a frame field — there is no login
/// available at `can_read` time to key on instead without adding a GitHub
/// call this cache exists to avoid.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct CallerDigest([u8; 32]);

impl CallerDigest {
    fn of(token: &SecretToken) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(token.expose().as_bytes());
        let digest = hasher.finalize();
        // SHA-256's output is always 32 bytes, so this length can never
        // mismatch — the copy is only here to move it into a fixed-size,
        // `Hash`-able array.
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        Self(out)
    }
}

impl fmt::Debug for CallerDigest {
    /// Redacted on principle, not because a hash is itself a secret — the
    /// same reasoning as `SecretToken`'s `Debug`, so a `tracing` call added
    /// later cannot regress this without also touching this impl.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CallerDigest(redacted)")
    }
}

/// A repo as a lookup key, never as something to show.
///
/// GitHub owner and repository names are case-insensitive, so
/// `Example-org/Example-repo` and `example-org/example-repo` are one repository and
/// one access answer. Keyed as they were spelled, every case variant a
/// daemon's answer invented missed the cache and cost the reader a GitHub
/// call. `RepoSlug` admits only ASCII, so ASCII folding is complete.
///
/// Deliberately no `Display`, `Debug` or accessor: an agent row keeps the
/// spelling its daemon sent, and the folded form has no way onto the wire or
/// into a log line.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RepoKey(Box<str>);

impl RepoKey {
    pub fn of(repo: &RepoSlug) -> Self {
        Self(repo.as_str().to_ascii_lowercase().into_boxed_str())
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    caller: CallerDigest,
    repo: RepoKey,
}

/// [`RepoAccess`] with the one variant this cache is not allowed to hold
/// removed at the type level. A function that wants to insert an
/// `Unverifiable` answer cannot: there is no such value of this type to pass
/// it, which is a stronger guarantee than a runtime check at the one call
/// site that inserts.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CachedAccess {
    Readable,
    NotReadable,
}

impl From<CachedAccess> for RepoAccess {
    fn from(value: CachedAccess) -> Self {
        match value {
            CachedAccess::Readable => RepoAccess::Readable,
            CachedAccess::NotReadable => RepoAccess::NotReadable,
        }
    }
}

/// The only place `RepoAccess::Unverifiable` is matched on the way into the
/// cache. Returns `None` for it, which is what keeps it out.
fn cacheable(access: RepoAccess) -> Option<CachedAccess> {
    match access {
        RepoAccess::Readable => Some(CachedAccess::Readable),
        RepoAccess::NotReadable => Some(CachedAccess::NotReadable),
        RepoAccess::Unverifiable => None,
    }
}

struct CacheEntry {
    access: CachedAccess,
    expires_at: Instant,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<CacheKey, CacheEntry>,
    /// Insertion order, for FIFO eviction. `insert` pushes a key here only
    /// when it was not already resident in `entries`, so refreshing an
    /// already-resident, still-live key does not push again. Without that
    /// de-duplication, repeatedly refreshing one small, hot set of keys would
    /// grow this queue forever even though `entries` stayed tiny — a real
    /// unbounded-memory bug an earlier version of this file had, since
    /// eviction was keyed on `entries.len()` and a hot key never pushes
    /// `entries` over the cap. Eviction is keyed on `order.len()` instead for
    /// exactly this reason: it is the one of the two that cannot shrink on
    /// its own between inserts (`entries` can shrink from a lazy expiry in
    /// `lookup`, leaving a stale key resident here until it is FIFO-evicted).
    ///
    /// This does not guarantee at most one occurrence per key: a key that
    /// expired, was lazily removed from `entries` by `lookup`, and is later
    /// asked about again gets a second, fresh push here alongside its
    /// not-yet-evicted stale one. That is harmless for the one property that
    /// matters — `order.len()` is what eviction caps, unconditionally, so the
    /// bound holds regardless — and costs at most an early eviction of a
    /// still-fresh entry, the same acceptable failure mode as any other
    /// FIFO-not-LRU eviction here.
    order: VecDeque<CacheKey>,
}

/// Bounded, FIFO-evicted cache of [`CachedAccess`] by [`CacheKey`].
///
/// Eviction policy (rule 44): capped at [`MAX_ENTRIES`] by capping
/// `order.len()` (see its doc comment for why that one and not
/// `entries.len()`), which in turn bounds `entries.len()`. Once full, the
/// longest-resident key is evicted first, expired or not. Expiry is also
/// checked lazily on every lookup, so an expired entry never survives past
/// the next read that touches it even if the cache is nowhere near full.
/// Never a true LRU — a hot key keeps its original queue position rather
/// than moving to the back — so the failure mode of eviction is an extra
/// GitHub call for something still fresh, never a wrong answer.
struct AccessCache {
    state: Mutex<CacheState>,
}

impl AccessCache {
    fn new() -> Self {
        Self {
            state: Mutex::new(CacheState::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CacheState> {
        // A panic elsewhere while this lock is held would poison it, but
        // `expect`/`unwrap` are denied outside tests (rule 39). Rather than
        // propagate that panic into every later caller of this cache,
        // `PoisonError::into_inner` recovers the guarded `CacheState` exactly
        // as the panicking thread left it and continues using it as-is —
        // this does not itself change what the cache returns; every
        // mutation this module makes while holding the lock completes in
        // full before anything that could panic runs, so there is nothing
        // for a real poisoning here to have left half-done.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lookup(&self, key: &CacheKey) -> Option<RepoAccess> {
        let mut state = self.lock();
        let entry = state.entries.get(key)?;
        if Instant::now() >= entry.expires_at {
            state.entries.remove(key);
            return None;
        }
        Some(entry.access.into())
    }

    fn insert(&self, key: CacheKey, access: CachedAccess, ttl: Duration) {
        // Overflow-safe (rule 40): `Instant + Duration` can panic on overflow.
        // A TTL that cannot be represented as a future instant is not cached
        // at all rather than risking that panic — the caller just asks GitHub
        // again next time, which is the same safe direction as never caching
        // `Unverifiable`.
        let Some(expires_at) = Instant::now().checked_add(ttl) else {
            return;
        };

        let mut state = self.lock();
        // Only a genuinely new key grows `order`. Refreshing one that is
        // already resident (the hot-key case a fan-out produces constantly)
        // must not, or `order` — and with it, effectively, resident memory —
        // grows without bound for as long as the process runs, no matter how
        // small `MAX_ENTRIES` is.
        let is_new_key = !state.entries.contains_key(&key);
        state
            .entries
            .insert(key.clone(), CacheEntry { access, expires_at });
        if is_new_key {
            state.order.push_back(key);
        }

        // Keyed on `order.len()`, not `entries.len()`: a key that expired and
        // was lazily removed by `lookup` still occupies a slot in `order`
        // until it is FIFO-evicted here, so `order` is the one of the two
        // that never shrinks on its own between inserts and is the correct
        // thing to cap.
        while state.order.len() > MAX_ENTRIES {
            let Some(oldest) = state.order.pop_front() else {
                break;
            };
            state.entries.remove(&oldest);
        }
    }
}

/// Wraps a [`Directory`], caching only [`Directory::can_read`] and only for
/// [`CachedAccess`]-representable answers. `login` and `is_member` pass
/// straight through.
pub struct CachingDirectory<D> {
    inner: D,
    cache: AccessCache,
    ttl: Duration,
}

impl<D> CachingDirectory<D> {
    /// `ttl` is clamped to [`MAX_TTL`] regardless of what is passed in — the
    /// hard ceiling lives here, in code, rather than only in whatever
    /// validates the configured value upstream.
    pub fn new(inner: D, ttl: Duration) -> Self {
        Self {
            inner,
            cache: AccessCache::new(),
            ttl: ttl.min(MAX_TTL),
        }
    }
}

impl<D: Directory> Directory for CachingDirectory<D> {
    async fn login(&self, token: &SecretToken) -> Result<String, Denied> {
        self.inner.login(token).await
    }

    async fn is_member(&self, token: &SecretToken) -> Result<bool, Denied> {
        self.inner.is_member(token).await
    }

    async fn can_read(&self, token: &SecretToken, repo: &RepoSlug) -> RepoAccess {
        // A TTL of zero is a legal explicit "disable caching" request (see
        // `parse_access_cache_ttl` in `main.rs`), not a cache that merely
        // expires the instant it is written. Handled here, before the cache
        // key is even built, so it means no hashing, no lock, no insert and
        // no push to the eviction queue — a real bypass, not a round trip
        // through a cache that always misses.
        if self.ttl.is_zero() {
            return self.inner.can_read(token, repo).await;
        }

        let key = CacheKey {
            caller: CallerDigest::of(token),
            repo: RepoKey::of(repo),
        };

        if let Some(cached) = self.cache.lookup(&key) {
            return cached;
        }

        let access = self.inner.can_read(token, repo).await;
        if let Some(cacheable) = cacheable(access) {
            self.cache.insert(key, cacheable, self.ttl);
        }
        access
    }

    /// Passed straight through, never cached here: the archive check keeps
    /// its own verdict per room (`archive::Verdicts`), and caching a pull
    /// request list per caller as well would only make that verdict staler.
    async fn recent_pulls(
        &self,
        token: &SecretToken,
        repo: &RepoSlug,
    ) -> Result<Vec<PullSummary>, PullsUnverifiable> {
        self.inner.recent_pulls(token, repo).await
    }

    fn known_access(&self, token: &SecretToken, repo: &RepoSlug) -> Option<RepoAccess> {
        if self.ttl.is_zero() {
            return None;
        }
        self.cache.lookup(&CacheKey {
            caller: CallerDigest::of(token),
            repo: RepoKey::of(repo),
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct CountingDirectory {
        calls: AtomicUsize,
        answer: RepoAccess,
    }

    impl CountingDirectory {
        fn new(answer: RepoAccess) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                answer,
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl Directory for CountingDirectory {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Ok("sandbank".to_owned())
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(true)
        }
        async fn can_read(&self, _token: &SecretToken, _repo: &RepoSlug) -> RepoAccess {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.answer
        }
    }

    /// Answers according to which token asks, so a test can prove the cache
    /// key includes the caller rather than just the repo.
    struct PerCallerDirectory {
        calls: AtomicUsize,
        readable_for: &'static str,
    }

    impl Directory for PerCallerDirectory {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Ok("sandbank".to_owned())
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(true)
        }
        async fn can_read(&self, token: &SecretToken, _repo: &RepoSlug) -> RepoAccess {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if token.expose() == self.readable_for {
                RepoAccess::Readable
            } else {
                RepoAccess::NotReadable
            }
        }
    }

    fn repo(slug: &str) -> RepoSlug {
        RepoSlug::try_from(slug).unwrap()
    }

    fn token(value: &str) -> SecretToken {
        SecretToken::new(value.to_owned())
    }

    /// Lists one merged pull request for the caller it was built for, and
    /// nothing for anyone else.
    struct Lists {
        for_token: &'static str,
        calls: AtomicUsize,
    }

    impl Directory for Lists {
        async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
            Ok("sandbank".to_owned())
        }
        async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
            Ok(true)
        }
        async fn recent_pulls(
            &self,
            token: &SecretToken,
            _repo: &RepoSlug,
        ) -> Result<Vec<PullSummary>, PullsUnverifiable> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if token.expose() != self.for_token {
                return Ok(Vec::new());
            }
            Ok(vec![PullSummary {
                head_ref: "feature/xyz-62-archive".to_owned(),
                into_default_branch: true,
                state: crate::auth::PullState::Merged,
            }])
        }
    }

    /// The router's directory is this cache over `Github`, and the trait's
    /// default for `recent_pulls` is "unverifiable". A cache that forgot to
    /// forward it would leave every ticket room open forever with nothing
    /// failing — so this pins the forward, with the caller's own token, and
    /// that nothing about it is cached.
    #[tokio::test]
    async fn pull_listing_passes_through_with_the_callers_own_token_uncached() {
        let cache = CachingDirectory::new(
            Lists {
                for_token: "ghp_atlas",
                calls: AtomicUsize::new(0),
            },
            Duration::from_secs(30),
        );
        let slug = repo("example-org/sealmesh");
        let listed = cache.recent_pulls(&token("ghp_atlas"), &slug).await;
        assert_eq!(listed.map(|pulls| pulls.len()), Ok(1));
        assert_eq!(
            cache.recent_pulls(&token("ghp_bay"), &slug).await,
            Ok(Vec::new()),
            "one caller's listing was served to another"
        );
        let _ = cache.recent_pulls(&token("ghp_atlas"), &slug).await;
        assert_eq!(cache.inner.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_readable_grant_is_cached_and_the_inner_directory_is_asked_once() {
        let inner = CountingDirectory::new(RepoAccess::Readable);
        let caching = CachingDirectory::new(inner, Duration::from_secs(60));
        let t = token("ghp_a");
        let r = repo("example-org/sealmesh");

        assert_eq!(caching.can_read(&t, &r).await, RepoAccess::Readable);
        assert_eq!(caching.can_read(&t, &r).await, RepoAccess::Readable);
        assert_eq!(caching.can_read(&t, &r).await, RepoAccess::Readable);

        assert_eq!(caching.inner.calls(), 1);
    }

    #[tokio::test]
    async fn a_negative_result_is_cached_no_less_than_a_positive_one() {
        let inner = CountingDirectory::new(RepoAccess::NotReadable);
        let caching = CachingDirectory::new(inner, Duration::from_secs(60));
        let t = token("ghp_a");
        let r = repo("example-org/sealmesh");

        assert_eq!(caching.can_read(&t, &r).await, RepoAccess::NotReadable);
        assert_eq!(caching.can_read(&t, &r).await, RepoAccess::NotReadable);

        assert_eq!(
            caching.inner.calls(),
            1,
            "a No should be cached just like a Yes"
        );
    }

    #[tokio::test]
    async fn case_variants_of_one_repo_share_one_entry_and_cost_one_check() {
        let inner = CountingDirectory::new(RepoAccess::Readable);
        let caching = CachingDirectory::new(inner, Duration::from_secs(60));
        let t = token("ghp_a");

        for spelling in [
            "Example-org/Example-repo",
            "example-org/example-repo",
            "Example-org/EXAMPLE-REPO",
        ] {
            assert_eq!(
                caching.can_read(&t, &repo(spelling)).await,
                RepoAccess::Readable
            );
        }

        assert_eq!(caching.inner.calls(), 1, "a case variant missed the cache");
        assert_eq!(caching.cache.lock().entries.len(), 1);
        // Folding is only for the key; a different repo is still a miss.
        caching
            .can_read(&t, &repo("example-org/example-repo-portal"))
            .await;
        assert_eq!(caching.inner.calls(), 2);
    }

    #[tokio::test]
    async fn known_access_reports_only_what_is_cached_and_never_asks() {
        let inner = CountingDirectory::new(RepoAccess::NotReadable);
        let caching = CachingDirectory::new(inner, Duration::from_secs(60));
        let t = token("ghp_a");
        let r = repo("example-org/sealmesh");

        assert_eq!(caching.known_access(&t, &r), None);
        caching.can_read(&t, &r).await;
        assert_eq!(
            caching.known_access(&t, &repo("EXAMPLE-ORG/Sealmesh")),
            Some(RepoAccess::NotReadable)
        );
        assert_eq!(
            caching.known_access(&token("ghp_other"), &r),
            None,
            "one caller's answer was known to another"
        );
        assert_eq!(caching.inner.calls(), 1);

        let uncached =
            CachingDirectory::new(CountingDirectory::new(RepoAccess::Readable), Duration::ZERO);
        uncached.can_read(&t, &r).await;
        assert_eq!(uncached.known_access(&t, &r), None);
    }

    #[tokio::test]
    async fn unverifiable_is_never_cached() {
        let inner = CountingDirectory::new(RepoAccess::Unverifiable);
        let caching = CachingDirectory::new(inner, Duration::from_secs(60));
        let t = token("ghp_a");
        let r = repo("example-org/sealmesh");

        assert_eq!(caching.can_read(&t, &r).await, RepoAccess::Unverifiable);
        assert_eq!(caching.can_read(&t, &r).await, RepoAccess::Unverifiable);
        assert_eq!(caching.can_read(&t, &r).await, RepoAccess::Unverifiable);

        assert_eq!(
            caching.inner.calls(),
            3,
            "an unverifiable answer must never settle into a cached one"
        );
    }

    #[tokio::test]
    async fn a_zero_ttl_disables_caching_entirely() {
        // Not "a TTL so short it always misses" — a real bypass. Three calls
        // must reach the inner directory three times, and nothing should
        // ever land in the cache's own state.
        let inner = CountingDirectory::new(RepoAccess::Readable);
        let caching = CachingDirectory::new(inner, Duration::ZERO);
        let t = token("ghp_a");
        let r = repo("example-org/sealmesh");

        assert_eq!(caching.can_read(&t, &r).await, RepoAccess::Readable);
        assert_eq!(caching.can_read(&t, &r).await, RepoAccess::Readable);
        assert_eq!(caching.can_read(&t, &r).await, RepoAccess::Readable);

        assert_eq!(
            caching.inner.calls(),
            3,
            "a TTL of zero must disable caching, not merely expire instantly"
        );
        assert_eq!(
            caching.cache.lock().entries.len(),
            0,
            "nothing should ever be inserted while caching is disabled"
        );
    }

    #[tokio::test]
    async fn one_callers_cached_grant_is_never_served_to_another() {
        let inner = PerCallerDirectory {
            calls: AtomicUsize::new(0),
            readable_for: "ghp_owner",
        };
        let caching = CachingDirectory::new(inner, Duration::from_secs(60));
        let r = repo("example-org/sealmesh");

        // Prime the cache with the owning caller's Readable answer.
        assert_eq!(
            caching.can_read(&token("ghp_owner"), &r).await,
            RepoAccess::Readable
        );

        // A different caller, same repo, must get their own answer — not the
        // cached Readable that belongs to someone else's token.
        assert_eq!(
            caching.can_read(&token("ghp_impostor"), &r).await,
            RepoAccess::NotReadable,
            "one caller's cached grant leaked to another caller"
        );

        assert_eq!(
            caching.inner.calls.load(Ordering::SeqCst),
            2,
            "the second caller must not be served from the first caller's cache entry"
        );
    }

    #[tokio::test]
    async fn an_expired_entry_is_asked_again() {
        let inner = CountingDirectory::new(RepoAccess::Readable);
        let caching = CachingDirectory::new(inner, Duration::from_millis(20));
        let t = token("ghp_a");
        let r = repo("example-org/sealmesh");

        assert_eq!(caching.can_read(&t, &r).await, RepoAccess::Readable);
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(caching.can_read(&t, &r).await, RepoAccess::Readable);

        assert_eq!(
            caching.inner.calls(),
            2,
            "a TTL that never expires is not a TTL"
        );
    }

    #[test]
    fn a_configured_ttl_above_the_ceiling_is_clamped_in_the_constructor() {
        let caching = CachingDirectory::new(
            CountingDirectory::new(RepoAccess::Readable),
            Duration::from_secs(u64::MAX),
        );
        assert_eq!(caching.ttl, MAX_TTL);
    }

    #[test]
    fn a_configured_ttl_under_the_ceiling_is_kept_as_is() {
        let ttl = Duration::from_secs(5);
        let caching = CachingDirectory::new(CountingDirectory::new(RepoAccess::Readable), ttl);
        assert_eq!(caching.ttl, ttl);
    }

    #[test]
    fn an_insert_whose_expiry_would_overflow_is_dropped_rather_than_panicking() {
        let cache = AccessCache::new();
        let key = CacheKey {
            caller: CallerDigest::of(&token("ghp_a")),
            repo: RepoKey::of(&repo("example-org/sealmesh")),
        };
        // `Duration::MAX` added to `Instant::now()` cannot be represented, so
        // this must not panic — and must not be resident afterwards either.
        cache.insert(key.clone(), CachedAccess::Readable, Duration::MAX);
        assert!(cache.lookup(&key).is_none());
    }

    #[test]
    fn the_cache_is_bounded_and_evicts_the_oldest_entry_first() {
        let cache = AccessCache::new();
        let ttl = Duration::from_secs(60);

        let make_key = |i: usize| CacheKey {
            caller: CallerDigest::of(&token(&format!("ghp_{i}"))),
            repo: RepoKey::of(&repo("example-org/sealmesh")),
        };

        for i in 0..MAX_ENTRIES {
            cache.insert(make_key(i), CachedAccess::Readable, ttl);
        }
        assert!(cache.lookup(&make_key(0)).is_some());

        // One more entry pushes the table over the cap, which must evict the
        // very first key inserted (FIFO) rather than growing unbounded.
        cache.insert(make_key(MAX_ENTRIES), CachedAccess::Readable, ttl);

        assert!(
            cache.lookup(&make_key(0)).is_none(),
            "the oldest entry should have been evicted to hold the cap"
        );
        assert!(cache.lookup(&make_key(MAX_ENTRIES)).is_some());
        assert_eq!(cache.lock().entries.len(), MAX_ENTRIES);
    }

    #[test]
    fn refreshing_one_hot_key_forever_does_not_grow_the_eviction_queue() {
        // A fan-out asking about the same handful of repos re-inserts the
        // same key over and over. An earlier version of `insert` pushed to
        // `order` unconditionally, so this loop would have left `order` with
        // 10x `MAX_ENTRIES` entries for one resident key — an unbounded
        // memory leak that the capacity-based eviction never caught, because
        // `entries.len()` never grew past 1.
        let cache = AccessCache::new();
        let key = CacheKey {
            caller: CallerDigest::of(&token("ghp_hot")),
            repo: RepoKey::of(&repo("example-org/sealmesh")),
        };

        for _ in 0..(MAX_ENTRIES * 10) {
            cache.insert(key.clone(), CachedAccess::Readable, Duration::from_secs(60));
        }

        let state = cache.lock();
        assert_eq!(state.entries.len(), 1);
        assert_eq!(
            state.order.len(),
            1,
            "refreshing an already-resident key must not grow the eviction queue"
        );
    }

    #[test]
    fn a_key_that_expires_and_is_never_refreshed_is_eventually_evicted() {
        // `lookup` removes an expired key from `entries` but leaves it in
        // `order` until it is FIFO-evicted. Capacity eviction is keyed on
        // `order.len()`, not `entries.len()`, specifically so a steady trickle
        // of once-asked, never-repeated keys — each of which leaves exactly
        // this kind of stale `order` entry behind — still gets bounded even
        // though `entries` itself never grows large.
        let cache = AccessCache::new();
        let stale = CacheKey {
            caller: CallerDigest::of(&token("ghp_once")),
            repo: RepoKey::of(&repo("example-org/sealmesh")),
        };
        cache.insert(
            stale.clone(),
            CachedAccess::Readable,
            Duration::from_millis(1),
        );
        std::thread::sleep(Duration::from_millis(20));
        assert!(cache.lookup(&stale).is_none(), "should have lazily expired");
        assert_eq!(
            cache.lock().entries.len(),
            0,
            "lookup removes the expired entry from `entries`"
        );

        let make_key = |i: usize| CacheKey {
            caller: CallerDigest::of(&token(&format!("ghp_fresh_{i}"))),
            repo: RepoKey::of(&repo("example-org/sealmesh")),
        };
        for i in 0..MAX_ENTRIES {
            cache.insert(make_key(i), CachedAccess::Readable, Duration::from_secs(60));
        }

        assert_eq!(
            cache.lock().order.len(),
            MAX_ENTRIES,
            "the stale leftover order entry must have been evicted along the way, \
             not left to accumulate forever"
        );
    }

    #[test]
    fn a_reinserted_expired_key_can_duplicate_in_the_queue_but_the_bound_holds() {
        // `lookup`'s lazy removal only touches `entries`, so a key that
        // expires and is later asked about again gets a second, fresh push
        // in `order` alongside its own not-yet-evicted stale occurrence —
        // the one case where `order` does not hold exactly one entry per
        // key. This must not let the queue grow past the cap.
        let cache = AccessCache::new();
        let hot = CacheKey {
            caller: CallerDigest::of(&token("ghp_hot")),
            repo: RepoKey::of(&repo("example-org/sealmesh")),
        };

        cache.insert(
            hot.clone(),
            CachedAccess::Readable,
            Duration::from_millis(1),
        );
        std::thread::sleep(Duration::from_millis(20));
        assert!(cache.lookup(&hot).is_none(), "should have lazily expired");

        cache.insert(hot.clone(), CachedAccess::Readable, Duration::from_secs(60));
        assert_eq!(
            cache.lock().order.len(),
            2,
            "a stale occurrence and a fresh one for the same key can coexist"
        );

        let make_key = |i: usize| CacheKey {
            caller: CallerDigest::of(&token(&format!("ghp_fill_{i}"))),
            repo: RepoKey::of(&repo("example-org/sealmesh")),
        };
        for i in 0..MAX_ENTRIES {
            cache.insert(make_key(i), CachedAccess::Readable, Duration::from_secs(60));
        }

        let state = cache.lock();
        assert!(
            state.order.len() <= MAX_ENTRIES,
            "the duplicate occurrence must not let the queue exceed the cap: {}",
            state.order.len()
        );
        assert!(
            state.entries.len() <= MAX_ENTRIES,
            "entries is bounded by order, so it cannot exceed the cap either: {}",
            state.entries.len()
        );
    }

    #[test]
    fn two_different_tokens_never_hash_to_the_same_digest_in_practice() {
        let a = CallerDigest::of(&token("ghp_atlas"));
        let b = CallerDigest::of(&token("ghp_bay"));
        assert_ne!(a, b);
    }

    #[test]
    fn a_digest_does_not_render_the_token_into_a_debug_dump() {
        let digest = CallerDigest::of(&token("ghp_realsecretvalue"));
        assert!(!format!("{digest:?}").contains("ghp_realsecretvalue"));
    }
}
