//! Room history in one SQLite file: the default store for a self-hosted router.
//!
//! Same contract as the DynamoDB store, checked by the same contract tests
//! (`history::tests::contract`):
//!
//! - one row per message, keyed `(room, seq)`, where `room` is
//!   [`partition_key`](super::partition_key);
//! - [`RoomHistory::append`] refuses a `seq` the room already holds as
//!   [`HistoryError::Collision`] and never overwrites;
//! - [`RoomHistory::page`] returns messages in `seq` order, skips anything past
//!   [`RETENTION`](super::RETENTION), and is windowed by the shared
//!   [`window_records`](super::window_records) so the byte cap and `next` are
//!   the ones every store uses;
//! - SQLite has no TTL, so [`SqliteHistory::sweep_expired`] deletes what the
//!   read path already refuses to serve. The router runs it on an interval.
//!
//! SQLite is synchronous: every call runs on Tokio's blocking pool, holding the
//! one connection behind a mutex. A router's write rate is bounded by its
//! budgets (a few posts per second per room), well inside one connection.

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::Context as _;
use rusqlite::{Connection, params};
use sealmesh_chat_wire::{Body, RoomId};
use sealmesh_proto::RoomSeq;
use sealmesh_wire::HostId;

use super::{
    BoxFuture, HistoryError, MAX_BODY_BYTES, MAX_PAGE_BYTES, MAX_PAGE_ITEMS, Page, Record,
    RoomHistory, kind_from_word, kind_word, now_epoch, partition_key, window_records,
};

/// The table and its expiry index. `STRICT` so a value of the wrong type is a
/// write error rather than a silent conversion; `WITHOUT ROWID` because the
/// primary key is the only way rows are read.
pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS room_history (
    room        TEXT    NOT NULL,
    seq         INTEGER NOT NULL,
    sender      TEXT    NOT NULL,
    kind        TEXT    NOT NULL,
    body        TEXT    NOT NULL,
    posted_at   INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,
    PRIMARY KEY (room, seq)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS room_history_expires_at ON room_history (expires_at);
";

/// The SQLite-backed store. Cheap to clone: clones share one connection.
#[derive(Clone)]
pub struct SqliteHistory {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteHistory {
    /// Opens (creating if needed) the database at `path` and its schema.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("opening room history at {}", path.display()))?;
        Self::prepare(conn).with_context(|| format!("preparing room history at {}", path.display()))
    }

    /// An in-memory database, for tests.
    pub fn open_in_memory() -> anyhow::Result<Self> {
        Self::prepare(Connection::open_in_memory().context("opening an in-memory database")?)
    }

    fn prepare(conn: Connection) -> anyhow::Result<Self> {
        // WAL: readers don't block the writer. A busy timeout rather than an
        // immediate SQLITE_BUSY if a sweep and a post meet.
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Runs `work` against the connection on Tokio's blocking pool.
    ///
    /// A poisoned mutex is recovered rather than propagated: the connection
    /// itself is still valid after a panic elsewhere, and SQLite's own
    /// transactions are what keep the data consistent.
    async fn with_conn<T, F>(&self, work: F) -> Result<T, HistoryError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, HistoryError> + Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let mut guard = conn
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            work(&mut guard)
        })
        .await
        .map_err(|_join_error| HistoryError::Unavailable)?
    }

    /// Deletes every message past its retention window as of `now` (epoch
    /// seconds), returning how many rows went. Expired rows are already
    /// invisible to `page`; this only reclaims the space. Uses the same bound
    /// the read path does: a message is expired once `expires_at <= now`.
    pub async fn sweep_expired(&self, now: u64) -> Result<u64, HistoryError> {
        let now = to_sql(now)?;
        self.with_conn(move |conn| {
            let deleted = conn
                .execute(
                    "DELETE FROM room_history WHERE expires_at <= ?1",
                    params![now],
                )
                .map_err(|_error| HistoryError::Unavailable)?;
            u64::try_from(deleted).map_err(|_overflow| HistoryError::Unavailable)
        })
        .await
    }
}

impl RoomHistory for SqliteHistory {
    /// Inserts one row. A primary-key violation — this room already holds
    /// `seq` — is [`HistoryError::Collision`] and nothing is overwritten;
    /// any other SQLite error is [`HistoryError::Unavailable`].
    fn append<'a>(&'a self, record: &'a Record) -> BoxFuture<'a, Result<(), HistoryError>> {
        Box::pin(async move {
            let seq = record.seq.0;
            let expires_at = record.expires_at().ok_or(HistoryError::Clock)?;
            // Owned copies: the write runs on the blocking pool, after this
            // borrow of `record` could have ended.
            let room = partition_key(&record.room);
            let sql_seq = to_sql(seq)?;
            let sender = record.from.to_string();
            let kind = kind_word(record.kind);
            let body = record.body.as_str().to_owned();
            let posted_at = to_sql(record.posted_at)?;
            let expires_at = to_sql(expires_at)?;
            self.with_conn(move |conn| {
                conn.execute(
                    // A plain INSERT, never OR REPLACE: the primary key is
                    // the whole of the overwrite protection.
                    "INSERT INTO room_history (room, seq, sender, kind, body, posted_at, expires_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![room, sql_seq, sender, kind, body, posted_at, expires_at],
                )
                .map(|_rows| ())
                .map_err(|error| {
                    if is_primary_key_violation(&error) {
                        HistoryError::Collision { seq }
                    } else {
                        HistoryError::Unavailable
                    }
                })
            })
            .await
        })
    }

    /// The highest `seq` stored for `room`, expired or not (exactly like an
    /// item the TTL hasn't deleted yet), or `None` for an empty room.
    fn highest_seq<'a>(
        &'a self,
        room: &'a RoomId,
    ) -> BoxFuture<'a, Result<Option<u64>, HistoryError>> {
        Box::pin(async move {
            let room = partition_key(room);
            self.with_conn(move |conn| {
                let highest: Option<i64> = conn
                    .query_row(
                        "SELECT MAX(seq) FROM room_history WHERE room = ?1",
                        params![room],
                        |row| row.get(0),
                    )
                    .map_err(|_error| HistoryError::Unavailable)?;
                highest
                    .map(|seq| {
                        u64::try_from(seq).map_err(|_negative| HistoryError::Malformed {
                            attribute: "seq",
                            seq: None,
                        })
                    })
                    .transpose()
            })
            .await
        })
    }

    /// Up to `limit` messages of `room` after `after`, in `seq` order, `limit`
    /// clamped to `1..=MAX_PAGE_ITEMS` the way the other stores clamp it. One
    /// row more than the limit is read to know whether there is more; each row
    /// is rebuilt strictly, and the window goes through the shared
    /// [`window_records`] so expiry, the byte cap and `next` are the ones every
    /// store uses.
    fn page<'a>(
        &'a self,
        room: &'a RoomId,
        after: Option<u64>,
        limit: i32,
    ) -> BoxFuture<'a, Result<Page, HistoryError>> {
        Box::pin(async move {
            let limit = limit.clamp(1, MAX_PAGE_ITEMS);
            let wanted = usize::try_from(limit).map_err(|_negative| HistoryError::Unavailable)?;
            let key = partition_key(room);
            // Sequence numbers start at 1, so "after 0" is the whole room.
            let after = to_sql(after.unwrap_or(0))?;
            let fetch = i64::from(limit) + 1;
            let rows = self
                .with_conn(move |conn| {
                    let mut statement = conn
                        .prepare_cached(
                            "SELECT seq, sender, kind, body, posted_at FROM room_history \
                             WHERE room = ?1 AND seq > ?2 ORDER BY seq LIMIT ?3",
                        )
                        .map_err(|_error| HistoryError::Unavailable)?;
                    let rows = statement
                        .query_map(params![key, after, fetch], |row| {
                            Ok(StoredRow {
                                seq: row.get(0)?,
                                sender: row.get(1)?,
                                kind: row.get(2)?,
                                body: row.get(3)?,
                                posted_at: row.get(4)?,
                            })
                        })
                        .map_err(|_error| HistoryError::Unavailable)?;
                    rows.collect::<Result<Vec<_>, _>>()
                        .map_err(|_error| HistoryError::Unavailable)
                })
                .await?;

            let more = rows.len() > wanted;
            let records = rows
                .into_iter()
                .take(wanted)
                .map(|row| row.into_record(room));
            window_records(records, more, now_epoch()?, MAX_PAGE_BYTES)
        })
    }
}

/// One row as SQLite returns it, before anything about it is trusted.
struct StoredRow {
    seq: i64,
    sender: String,
    kind: String,
    body: String,
    posted_at: i64,
}

impl StoredRow {
    /// The row as a [`Record`] of `room`, or a refusal. Strict the same way the
    /// DynamoDB reader is: every value goes through its own validating
    /// constructor, and a row that fails one is refused, never skipped. Errors
    /// name the column and the sequence number, never the value.
    fn into_record(self, room: &RoomId) -> Result<Record, HistoryError> {
        let seq = u64::try_from(self.seq).ok().filter(|&seq| seq != 0).ok_or(
            HistoryError::Malformed {
                attribute: "seq",
                seq: None,
            },
        )?;
        let at = Some(seq);
        let malformed = |attribute| HistoryError::Malformed { attribute, seq: at };

        let from = HostId::try_from(self.sender.as_str()).map_err(|_refused| malformed("from"))?;
        let kind = kind_from_word(&self.kind).ok_or_else(|| malformed("kind"))?;
        // Size first, then the constructor, which walks the string.
        if self.body.len() > MAX_BODY_BYTES {
            return Err(HistoryError::TooLarge {
                bytes: self.body.len(),
            });
        }
        let body = Body::new(self.body).map_err(|_refused| malformed("body"))?;
        let posted_at =
            u64::try_from(self.posted_at).map_err(|_negative| malformed("posted_at"))?;

        Ok(Record {
            room: room.clone(),
            seq: RoomSeq(seq),
            from,
            kind,
            body,
            posted_at,
        })
    }
}

/// A `u64` as SQLite's signed 64-bit integer. Every value written here — a
/// sequence number, an epoch second — is far below `i64::MAX`; one that isn't
/// is refused rather than wrapped.
fn to_sql(value: u64) -> Result<i64, HistoryError> {
    i64::try_from(value).map_err(|_overflow| HistoryError::Unavailable)
}

fn is_primary_key_violation(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use sealmesh_chat_wire::{MessageKind, Scope};
    use sealmesh_wire::ProjectId;

    use super::*;
    use crate::history::RETENTION;

    fn room() -> RoomId {
        RoomId::new(ProjectId::try_from("example-repo").unwrap(), Scope::Project)
    }

    fn record(seq: u64, posted_at: u64) -> Record {
        Record {
            room: room(),
            seq: RoomSeq(seq),
            from: HostId::try_from("sandbank").unwrap(),
            kind: MessageKind::Claim,
            body: Body::new(format!("message {seq}")).unwrap(),
            posted_at,
        }
    }

    fn scratch_path(tag: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("sealmesh-sqlite-{tag}-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("history.sqlite")
    }

    #[tokio::test]
    async fn history_survives_closing_and_reopening_the_file() {
        let path = scratch_path("reopen");
        let now = now_epoch().unwrap();
        {
            let store = SqliteHistory::open(&path).unwrap();
            store.append(&record(1, now)).await.unwrap();
            store.append(&record(2, now)).await.unwrap();
        }
        let reopened = SqliteHistory::open(&path).unwrap();
        assert_eq!(reopened.highest_seq(&room()).await.unwrap(), Some(2));
        let page = reopened.page(&room(), None, MAX_PAGE_ITEMS).await.unwrap();
        assert_eq!(page.messages, vec![record(1, now), record(2, now)]);
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[tokio::test]
    async fn the_sweep_deletes_expired_messages_and_keeps_live_ones() {
        let store = SqliteHistory::open_in_memory().unwrap();
        let now = now_epoch().unwrap();
        let expired = now - RETENTION.as_secs() - 60;
        store.append(&record(1, expired)).await.unwrap();
        store.append(&record(2, now)).await.unwrap();
        assert_eq!(store.sweep_expired(now).await.unwrap(), 1);
        assert_eq!(
            store.sweep_expired(now).await.unwrap(),
            0,
            "nothing left to sweep"
        );
        // The expired row is gone from the table itself, not only from reads.
        assert_eq!(store.highest_seq(&room()).await.unwrap(), Some(2));
        let page = store.page(&room(), None, MAX_PAGE_ITEMS).await.unwrap();
        assert_eq!(page.messages, vec![record(2, now)]);
    }

    #[tokio::test]
    async fn a_row_its_constructor_would_refuse_is_refused_not_skipped() {
        let store = SqliteHistory::open_in_memory().unwrap();
        let now = now_epoch().unwrap();
        store.append(&record(1, now)).await.unwrap();
        let key = partition_key(&room());
        let ok_now = to_sql(now).unwrap();
        store
            .with_conn(move |conn| {
                conn.execute(
                    "INSERT INTO room_history (room, seq, sender, kind, body, posted_at, expires_at) \
                     VALUES (?1, 2, 'sandbank', 'shout', 'x', ?2, ?2)",
                    params![key, ok_now],
                )
                .map(|_| ())
                .map_err(|_| HistoryError::Unavailable)
            })
            .await
            .unwrap();
        assert_eq!(
            store.page(&room(), None, MAX_PAGE_ITEMS).await,
            Err(HistoryError::Malformed {
                attribute: "kind",
                seq: Some(2)
            })
        );
    }
}
