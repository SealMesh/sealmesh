//! Room message history at rest.
//!
//! This is the first thing in this system that is stored. Everything else the
//! router holds is in-process and dies with the task: the connection registry,
//! room membership, every budget. A room's messages cannot be, because a room
//! exists so an agent can join late and read what happened, and an in-process
//! table loses that on every deploy.
//!
//! # What is stored, and what is still not
//!
//! Chat, and nothing else. One item per delivered message: the room, the
//! router's sequence number, the credential-derived host that posted it, the
//! message kind, the body, when it was posted, and when it expires. No
//! snapshot, no `Agent`, no cached answer, no last-known reply — the "nothing
//! at rest" property is *narrowed to chat*, not dropped, and
//! [`Record`] is the whole of what narrows it: there is no field here anything
//! derived from a `Snapshot` could be assigned to.
//!
//! # Retention is seven days, enforced twice
//!
//! [`RETENTION`] is written into every item as `expires_at`, in epoch seconds,
//! at write time. DynamoDB's TTL deletes the item after that, so a router
//! that is never deployed again still eventually stops holding last week's
//! messages, with no job, no sweep and no code of ours to run for it. But the
//! TTL sweep is best-effort and can lag by days, so it is not the
//! only enforcement: [`page`](RoomHistory::page) filters on `expires_at`
//! too, and a message past it is never served, however long the table has
//! not gotten around to deleting it. `RETENTION` is a constant with a test
//! rather than a configuration knob because it is the number the README's
//! privacy promise quotes, and a promise an operator can retune by
//! environment variable is not a promise.
//!
//! # The IAM grant is the design
//!
//! The router's task role holds exactly `dynamodb:PutItem` and
//! `dynamodb:Query` on one table. No `UpdateItem`, no `DeleteItem`, no `Scan`,
//! no `BatchWriteItem`, no wildcard. So:
//!
//! - A sequence number is **not** an atomic counter item, because incrementing
//!   one needs `UpdateItem`. It is a descending [`RoomHistory::highest_seq`]
//!   query when a room first forms in this process, then an in-process
//!   counter, then a conditional write.
//! - [`RoomHistory::append`] is a conditional `PutItem` — `attribute_not_exists`
//!   on the sort key — so a sequence number that is somehow already taken
//!   makes the write **refuse** rather than overwrite a message. Losing
//!   somebody's message to a silent overwrite is the one outcome this store
//!   must not have, and the grant cannot express a repair, so it expresses a
//!   refusal instead.
//! - Nothing here deletes. Expiry is the TTL's — the read path only ever
//!   withholds an expired item, never removes one.
//!
//! # Reads are hostile input, and capped before the parser
//!
//! The router writes these items, but a read path that trusts what it wrote is
//! a read path that trusts whatever is in the table. So every item is parsed
//! strictly (rules 42, 44, 45): the attribute set must be exactly the one
//! [`Record`] defines — no unknown attribute, the way a declared record is
//! strict because we own the format — every string goes through its validating
//! constructor, and the body's size is checked *before* `Body::new` sees it.
//! Counts and bytes are bounded per page ([`MAX_PAGE_ITEMS`],
//! [`MAX_PAGE_BYTES`]) and per backfill ([`MAX_PAGES`],
//! [`MAX_BACKFILL_BYTES`]), and each byte cap is below what the count cap
//! alone would allow, so both are enforced rather than one being a claim.
//! An item past its `expires_at` is a third kind of hostile-by-accident input
//! — not malformed, just late — and is skipped rather than returned
//!. A page or a backfill whose whole window is expired still
//! reports where to resume, from what it scanned rather than what it kept, so
//! a room that has been quiet past its retention window does not strand a
//! reader mid-history.
//!
//! # Errors carry a category, never bytes
//!
//! [`HistoryError`] carries a category, a sequence number we chose or read as
//! a number, and a `&'static str` attribute name that is a literal in this
//! file. It never carries the value an attribute held, and never the SDK's own
//! message, which quotes what it choked on (rules 14, 26).

#[cfg(feature = "dynamodb")]
use std::collections::HashMap;
use std::{
    future::Future,
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "dynamodb")]
use aws_sdk_dynamodb::types::AttributeValue;

pub mod sqlite;
use sealmesh_chat_wire::{Body, MAX_BODY_CHARS, MessageKind, RoomId, Scope};
use sealmesh_proto::RoomSeq;
use sealmesh_wire::HostId;

#[cfg(feature = "dynamodb")]
/// The environment variable naming the table. Required, with no default.
///
/// Rule 8's reasoning applies one step over: this does not decide *who* gets
/// in, it decides *where* a room's messages go. A default would be a guess at
/// a table name, and guessing wrong writes an org's coordination history
/// somewhere nobody is looking, or silently nowhere. So a missing or blank
/// value refuses to start rather than being filled in.
pub const TABLE_VAR: &str = "DYNAMODB_ROOM_HISTORY";

/// How long a message is kept. Seven days, written into every item as
/// `expires_at`.
///
/// A coordination room's value is measured in hours: the reason to keep
/// anything at all is a late joiner and a human reading back over a piece of
/// work, and neither needs last month. This is the main quantitative term in
/// the README's privacy promise, which is why it is here as a constant with a
/// test rather than anywhere an operator can retune it.
pub const RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Most items one `Query` may return.
///
/// A page is a bounded read triggered by a remote request (rule 44), so it is
/// bounded in count as well as bytes. Sixty-four messages is a useful chunk of
/// a room's recent history. Short messages are bounded by this; long ones by
/// [`MAX_PAGE_BYTES`], which a page of large bodies reaches first.
pub const MAX_PAGE_ITEMS: i32 = 64;

/// Most bytes of message body one page may keep before it stops and reports
/// more to come. Measured on the bodies as stored, after each item's own size
/// check and constructor, and the page is cut *before* the item that would
/// cross it.
///
/// A real second cap, not a floor: [`MAX_PAGE_ITEMS`] items of up to
/// [`MAX_BODY_BYTES`] each would come to 1,024,000 bytes, four times this, so
/// a page of large bodies is cut at sixteen of them. Half a frame's
/// worth of raw body, so what one store page hands the reply path stays
/// proportionate to the one frame it can fill. It never cuts a page to nothing,
/// which would read as the end of the room: one body always fits, checked at
/// compile time below.
pub const MAX_PAGE_BYTES: usize = 256 * 1024;

/// Most pages one [`backfill`] may read. The count bound on a paginated read:
/// without it, "keep asking until the store says it is done" is an unbounded
/// read a remote request can trigger.
pub const MAX_PAGES: usize = 16;

/// Most bytes of message body one [`backfill`] may keep across all its
/// pages, cut before the message that would cross it (rule 44).
///
/// Without it, [`MAX_PAGES`] full pages would keep 4 MiB of bodies for one
/// call. The live reply path is also cut by its frame check, measured on
/// escaped JSON and so never looser than this; this is the bound that holds
/// for every caller, including one without a frame to fill. Half a MiB is
/// the frame cap, the most one reply could carry anyway.
pub const MAX_BACKFILL_BYTES: usize = 512 * 1024;

/// Most bytes one persisted body may hold before it is refused unparsed.
///
/// `Body` caps at [`MAX_BODY_CHARS`] *characters*, and a character is at most
/// four UTF-8 bytes, so this is the largest a legitimate body can be. Checked
/// before the constructor, so an oversized string is refused rather than
/// counted (rule 42).
pub const MAX_BODY_BYTES: usize = MAX_BODY_CHARS * 4;

// A single body always fits under both byte caps, so neither can cut a read
// to nothing — which a page would report as the end of the room, and a
// backfill as a message too large to ever send.
const _: () = assert!(MAX_BODY_BYTES <= MAX_PAGE_BYTES && MAX_PAGE_BYTES <= MAX_BACKFILL_BYTES);

#[cfg(feature = "dynamodb")]
/// The attributes a history item has, and the only ones it may have.
///
/// Read strictly: an item carrying an attribute not in this list is refused
/// rather than having the extra discarded. Rule 43 in the strict direction —
/// nobody else owns this format, so accepting an unknown attribute would teach
/// a future writer that the format is wider than it is.
const ATTRIBUTES: [&str; 7] = [
    "room",
    "seq",
    "from",
    "kind",
    "body",
    "posted_at",
    "expires_at",
];

/// A future this trait's methods return. Spelled out rather than left as
/// `impl Future` so [`RoomHistory`] is object-safe: the router holds one
/// `Arc<dyn RoomHistory>` so that `Rooms` — and `App` above it — stay
/// non-generic, and a test can swap a fake in without a type parameter
/// reaching every call site that touches a room.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// One message, as it is stored.
///
/// Every field is either a validated newtype, a closed enum, or a number.
/// That is the same discipline the wire crate holds, for the same reason, with
/// the one exception the chat protocol is built around: `body` is prose, capped
/// and typed by `Body`. There is deliberately no field here that anything
/// derived from a `Snapshot` could be assigned to.
///
/// `Debug` is safe to derive: `Body`'s own `Debug` is redacted, so a `Record`
/// cannot put a message on a log line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub room: RoomId,
    pub seq: RoomSeq,
    pub from: HostId,
    pub kind: MessageKind,
    pub body: Body,
    /// When the router accepted the post, in epoch seconds.
    pub posted_at: u64,
}

impl Record {
    /// When this message stops existing, in epoch seconds: [`RETENTION`] after
    /// it was posted.
    ///
    /// `checked_add` rather than `+`: a `posted_at` near the end of the range
    /// would wrap in release and write an `expires_at` in the past, which the
    /// TTL would read as "delete this immediately" (rule 40).
    pub fn expires_at(&self) -> Option<u64> {
        self.posted_at.checked_add(RETENTION.as_secs())
    }
}

/// Why a history operation did not succeed.
///
/// Categories, a number, and attribute names that are literals in this file.
/// Nothing here interpolates a value read from the table or a message from the
/// SDK (rules 14, 26).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HistoryError {
    /// The store could not be reached, or refused the operation for a reason
    /// that is not a collision. Deliberately one category: a throttle, a
    /// timeout, a credential problem and a missing table are the same fact to
    /// the caller, which is "this message is not recorded".
    #[error("the room history store could not be reached")]
    Unavailable,
    /// The conditional write refused: an item already holds this room's `seq`.
    /// The message was **not** written, and nothing was overwritten.
    #[error("sequence {seq} is already used in that room")]
    Collision { seq: u64 },
    /// A stored item this build will not trust: a missing attribute, one of
    /// the wrong type, an unknown attribute, or a value its constructor
    /// refused. Carries which attribute and, when it is known, the sequence
    /// number of the item — never the value.
    #[error("a stored message has an unusable {attribute}")]
    Malformed {
        attribute: &'static str,
        seq: Option<u64>,
    },
    /// A stored value was refused on size before it was parsed.
    #[error("a stored message is {bytes} bytes, over the limit")]
    TooLarge { bytes: usize },
    /// The clock is before the Unix epoch, so no retention could be computed.
    /// Refused rather than defaulted: an item with a wrong `expires_at` is
    /// either kept too long or deleted at once, and both are worse than a
    /// refused post.
    #[error("the system clock is before the Unix epoch")]
    Clock,
}

/// A page of a room's history, oldest first.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Page {
    pub messages: Vec<Record>,
    /// The sequence number to resume after, when there may be more. `None`
    /// when this page reached the end of the room's history.
    pub next: Option<u64>,
}

/// Where a room's messages are kept.
///
/// A trait for the same reason `Directory` is one: the real implementation
/// talks to a service over the network, so every test in this crate runs
/// against a fake and no test needs AWS. The only place a real client is built
/// is `main`.
pub trait RoomHistory: Send + Sync {
    /// Writes one message, refusing rather than overwriting if its sequence
    /// number is already used in that room.
    fn append<'a>(&'a self, record: &'a Record) -> BoxFuture<'a, Result<(), HistoryError>>;

    /// The highest sequence number this room has ever persisted, or `None` for
    /// a room with no history. One descending query, limited to one item.
    fn highest_seq<'a>(
        &'a self,
        room: &'a RoomId,
    ) -> BoxFuture<'a, Result<Option<u64>, HistoryError>>;

    /// One page of a room's history in sequence order, starting after `after`.
    /// `limit` is clamped to [`MAX_PAGE_ITEMS`].
    fn page<'a>(
        &'a self,
        room: &'a RoomId,
        after: Option<u64>,
        limit: i32,
    ) -> BoxFuture<'a, Result<Page, HistoryError>>;
}

/// Reads a room's history from `after`, following pages until the room is
/// exhausted or a cap stops it.
///
/// The bounded form of "read it all": at most [`MAX_PAGES`] pages and
/// [`MAX_BACKFILL_BYTES`] of body in all, so a backfill of a room with a long
/// history returns a bounded prefix and says where to resume, rather than
/// reading until something runs out. The returned `next` is `Some` when a cap
/// stopped it, which is the caller's signal to ask again — not an error.
pub async fn backfill(
    store: &dyn RoomHistory,
    room: &RoomId,
    after: Option<u64>,
) -> Result<Page, HistoryError> {
    backfill_until(store, room, after, |_record| true).await
}

/// [`backfill`], also stopping at the first message `fits` refuses.
///
/// `fits` is asked about each message in order and says whether it still
/// belongs in this read. The router's backfill reply is one frame, and the
/// frame cap is measured after JSON escaping, which [`MAX_PAGE_BYTES`] is
/// not — so the reply path passes a running frame-size check here, and the
/// read stops at the message that would overflow it rather than reading
/// sixteen pages to throw most of them away.
///
/// When `fits` or [`MAX_BACKFILL_BYTES`] stops the read, `next` is the last
/// message kept, so the next request resumes at the refused one. A read whose
/// very first message does not fit could never make progress, so it is
/// refused as [`HistoryError::TooLarge`] rather than answered with nothing and
/// the same cursor forever.
pub async fn backfill_until(
    store: &dyn RoomHistory,
    room: &RoomId,
    after: Option<u64>,
    mut fits: impl FnMut(&Record) -> bool + Send,
) -> Result<Page, HistoryError> {
    let mut messages: Vec<Record> = Vec::new();
    let mut kept_bytes = 0usize;
    let mut cursor = after;
    for _ in 0..MAX_PAGES {
        let page = store.page(room, cursor, MAX_PAGE_ITEMS).await?;
        for record in page.messages {
            let bytes = kept_bytes.saturating_add(record.body.as_str().len());
            if bytes > MAX_BACKFILL_BYTES || !fits(&record) {
                return match messages.last() {
                    Some(kept) => Ok(Page {
                        next: Some(kept.seq.0),
                        messages,
                    }),
                    None => Err(HistoryError::TooLarge {
                        bytes: record.body.as_str().len(),
                    }),
                };
            }
            kept_bytes = bytes;
            messages.push(record);
        }
        match page.next {
            // More to come, and we know where to resume — even when this
            // page's own messages came back empty because everything in it
            // had expired: the read path filters on `expires_at`,
            // so a page can legitimately return nothing yet still have more
            // room history beyond it. `page()` always advances `next` from
            // what it actually scanned, not from what survived the filter, so
            // this still makes progress each iteration and the `MAX_PAGES`
            // bound above still caps the total work regardless.
            //
            // Guarded by strictly-forward progress: every `page()` in this
            // file derives `next` from a `seq > after` key condition, so it
            // cannot regress or repeat today, but a bound that only holds
            // because of how the current store happens to be written is not
            // a bound.
            Some(next) if cursor.is_none_or(|previous| next > previous) => cursor = Some(next),
            None => {
                return Ok(Page {
                    messages,
                    next: None,
                });
            }
            // A `next` that failed to advance is not a legitimate end of
            // history — treating it as one would read a store defect as a
            // room that has been quiet, silently truncating what a caller
            // sees. Refused instead, the same category every other read
            // failure in this file answers with.
            Some(_stuck) => return Err(HistoryError::Unavailable),
        }
    }
    Ok(Page {
        messages,
        next: cursor,
    })
}

/// The partition key for a room.
///
/// Injective, and cheaply so: `#` is in no identifier's charset — `ProjectId`
/// and `HostId` are lowercase, digits and `-`; a `RepoSlug` adds `.`, `_` and
/// one `/`; a `TicketId` is an uppercase prefix, `-`, and digits — so no
/// component can contain the separator and no two distinct rooms can collide
/// on one key.
///
/// There is deliberately no function that parses this back. A read is always
/// for a room the caller already has, so the read path *compares* the stored
/// key against the expected one instead, which is a check rather than a
/// parser, and one less thing to get wrong on hostile input.
pub fn partition_key(room: &RoomId) -> String {
    match &room.scope {
        Scope::Project => format!("{}#project", room.project),
        Scope::Repo(repo) => format!("{}#repo#{repo}", room.project),
        Scope::Ticket(ticket) => format!("{}#ticket#{ticket}", room.project),
    }
}

#[cfg(feature = "dynamodb")]
/// The table name from the environment: required, never defaulted.
///
/// An unset, blank or non-UTF-8 value is an error, so the router does not
/// start. The value is the operator's own, so quoting it back would be safe —
/// but there is nothing to quote, since the only failures are "absent" and
/// "empty".
pub fn table_from_env(value: Result<String, std::env::VarError>) -> anyhow::Result<String> {
    let table = match value {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => {
            anyhow::bail!("{TABLE_VAR} is not set, so room messages would have nowhere to go")
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            anyhow::bail!("{TABLE_VAR} is not valid UTF-8")
        }
    };
    let table = table.trim().to_owned();
    if table.is_empty() {
        anyhow::bail!("{TABLE_VAR} is empty, so room messages would have nowhere to go");
    }
    Ok(table)
}

/// Now, in epoch seconds.
///
/// Fallible rather than defaulted: a clock before the epoch cannot produce a
/// retention deadline, and inventing one would write an item that either never
/// expires or expires at once.
pub fn now_epoch() -> Result<u64, HistoryError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .map_err(|_before_epoch| HistoryError::Clock)
}

/// Whether `record` is past its retention window as of `now` — XYZ-108.
///
/// DynamoDB's TTL deletion is best-effort and can lag by days, so a `Query`
/// can still return an item this old. The read path is the second place that
/// enforces the seven-day bound, not the only one, and it uses the same
/// arithmetic [`Dynamo::put`] used to write `expires_at` in the first place:
/// `posted_at` plus [`RETENTION`]. A record whose expiry cannot even be
/// computed (`posted_at` near the end of the range) is treated as expired
/// rather than kept — refusing to serve is the safe direction when the bound
/// itself cannot be checked (rule 40's spirit).
fn is_expired(record: &Record, now: u64) -> bool {
    record
        .expires_at()
        .is_none_or(|expires_at| expires_at <= now)
}

fn kind_word(kind: MessageKind) -> &'static str {
    match kind {
        MessageKind::Question => "question",
        MessageKind::Claim => "claim",
        MessageKind::Handoff => "handoff",
        MessageKind::Ack => "ack",
    }
}

/// The four words [`kind_word`] writes, and nothing else. An explicit closed
/// match rather than serde over a value from the table: the set is four items,
/// and a `match` cannot be widened by accident.
fn kind_from_word(word: &str) -> Option<MessageKind> {
    match word {
        "question" => Some(MessageKind::Question),
        "claim" => Some(MessageKind::Claim),
        "handoff" => Some(MessageKind::Handoff),
        "ack" => Some(MessageKind::Ack),
        _ => None,
    }
}

#[cfg(feature = "dynamodb")]
/// The DynamoDB-backed store. One table, two verbs.
pub struct Dynamo {
    client: aws_sdk_dynamodb::Client,
    table: String,
}

#[cfg(feature = "dynamodb")]
impl Dynamo {
    pub fn new(client: aws_sdk_dynamodb::Client, table: String) -> Self {
        Self { client, table }
    }

    async fn put(&self, record: &Record) -> Result<(), HistoryError> {
        let expires_at = record.expires_at().ok_or(HistoryError::Clock)?;
        let result = self
            .client
            .put_item()
            .table_name(&self.table)
            .item("room", AttributeValue::S(partition_key(&record.room)))
            .item("seq", AttributeValue::N(record.seq.0.to_string()))
            .item("from", AttributeValue::S(record.from.to_string()))
            .item("kind", AttributeValue::S(kind_word(record.kind).to_owned()))
            .item("body", AttributeValue::S(record.body.as_str().to_owned()))
            .item("posted_at", AttributeValue::N(record.posted_at.to_string()))
            .item("expires_at", AttributeValue::N(expires_at.to_string()))
            // "No item with this (room, seq) exists." On a `PutItem` the
            // condition is evaluated against the item the key addresses, so
            // this is the whole of the overwrite protection: a sequence number
            // that is somehow already taken refuses instead of replacing a
            // message.
            .condition_expression("attribute_not_exists(#seq)")
            .expression_attribute_names("#seq", "seq")
            .send()
            .await;

        match result {
            Ok(_output) => Ok(()),
            Err(error) => {
                // Only the collision is told apart, and only through the
                // SDK's own predicate. The error's message is never read:
                // it renders the request, and this must not put the table's
                // contents or a credential on a log line (rules 14, 26, 27).
                if error
                    .as_service_error()
                    .is_some_and(|service| service.is_conditional_check_failed_exception())
                {
                    Err(HistoryError::Collision { seq: record.seq.0 })
                } else {
                    Err(HistoryError::Unavailable)
                }
            }
        }
    }
}

#[cfg(feature = "dynamodb")]
impl RoomHistory for Dynamo {
    fn append<'a>(&'a self, record: &'a Record) -> BoxFuture<'a, Result<(), HistoryError>> {
        Box::pin(self.put(record))
    }

    fn highest_seq<'a>(
        &'a self,
        room: &'a RoomId,
    ) -> BoxFuture<'a, Result<Option<u64>, HistoryError>> {
        Box::pin(async move {
            let key = partition_key(room);
            let answer = self
                .client
                .query()
                .table_name(&self.table)
                .key_condition_expression("#room = :room")
                .expression_attribute_names("#room", "room")
                .expression_attribute_values(":room", AttributeValue::S(key))
                // Descending, one item: the room's highest sequence number.
                .scan_index_forward(false)
                .limit(1)
                // Only the sort key is read. The bodies of a room's history
                // are not needed to continue its numbering, and not asking for
                // them is both the cheaper read and the smaller exposure.
                .projection_expression("#seq")
                .expression_attribute_names("#seq", "seq")
                .send()
                .await
                .map_err(|_error| HistoryError::Unavailable)?;

            match answer.items().first() {
                None => Ok(None),
                Some(item) => number(item, "seq", None).map(Some),
            }
        })
    }

    fn page<'a>(
        &'a self,
        room: &'a RoomId,
        after: Option<u64>,
        limit: i32,
    ) -> BoxFuture<'a, Result<Page, HistoryError>> {
        Box::pin(async move {
            let key = partition_key(room);
            // The count cap is applied to the request, so the store never
            // returns more than this many items — a limit applied after the
            // response has been parsed is not a limit (rule 42).
            let limit = limit.clamp(1, MAX_PAGE_ITEMS);
            let mut request = self
                .client
                .query()
                .table_name(&self.table)
                .expression_attribute_names("#room", "room")
                .expression_attribute_values(":room", AttributeValue::S(key.clone()))
                .scan_index_forward(true)
                .limit(limit);

            request = match after {
                None => request.key_condition_expression("#room = :room"),
                Some(after) => request
                    .key_condition_expression("#room = :room AND #seq > :after")
                    .expression_attribute_names("#seq", "seq")
                    .expression_attribute_values(":after", AttributeValue::N(after.to_string())),
            };

            let answer = request
                .send()
                .await
                .map_err(|_error| HistoryError::Unavailable)?;

            let more = answer.last_evaluated_key().is_some();
            let now = now_epoch()?;
            window_page(answer.items(), room, &key, more, now, MAX_PAGE_BYTES)
        })
    }
}

#[cfg(feature = "dynamodb")]
/// The windowing decision behind [`Dynamo::page`]: turns one `Query`
/// response's raw items into a [`Page`], filtering expired records and
/// deciding `next`. Pulled out as a pure function, over the same items a
/// `QueryOutput` exposes, so it is directly testable with the existing
/// `item`/`good_item` test helpers — the real DynamoDB path had no
/// test of its own before this, which meant deleting the expiry filter from
/// `Dynamo::page` left every test green.
///
/// `more` is whether the store said there is more beyond this window
/// (`last_evaluated_key().is_some()` on the real client). `now` is the
/// caller's clock read, taken once per page rather than per item. `cap` is
/// the byte ceiling ([`MAX_PAGE_BYTES`] in production); a parameter, not the
/// constant directly, so a test can inject a small one.
fn window_page(
    items: &[HashMap<String, AttributeValue>],
    room: &RoomId,
    key: &str,
    more: bool,
    now: u64,
    cap: usize,
) -> Result<Page, HistoryError> {
    window_records(
        items.iter().map(|item| read_item(item, room, key)),
        more,
        now,
        cap,
    )
}

/// One store window of `records`, in sequence order, as a [`Page`]: expired
/// records skipped, and the page cut before the first kept record
/// that would take its bodies past `cap`. Shared by the real store,
/// through [`window_page`], and the test one, so the expiry filter and the
/// byte cap a test exercises are the ones the router runs.
///
/// `next` is the last record kept when the cap cut the page, and otherwise,
/// when the store has more, the last record *scanned* — so a window that
/// filters to nothing still advances rather than stranding the rest of the
/// room's history behind it.
fn window_records(
    records: impl IntoIterator<Item = Result<Record, HistoryError>>,
    more: bool,
    now: u64,
    cap: usize,
) -> Result<Page, HistoryError> {
    let mut messages = Vec::new();
    let mut bytes = 0usize;
    let mut stopped_early = false;
    let mut last_scanned: Option<u64> = None;
    for record in records {
        let record = record?;
        last_scanned = Some(record.seq.0);
        if is_expired(&record, now) {
            // Past its 7-day expiry but not yet deleted by the TTL
            //. Not counted toward the byte cap and not kept.
            continue;
        }
        bytes = bytes.saturating_add(record.body.as_str().len());
        if bytes > cap {
            // The byte ceiling stops the page here and reports where to
            // resume, rather than returning a page over the cap.
            stopped_early = true;
            break;
        }
        messages.push(record);
    }

    let next = if stopped_early {
        messages.last().map(|record| record.seq.0)
    } else if more {
        last_scanned
    } else {
        None
    };
    Ok(Page { messages, next })
}

#[cfg(feature = "dynamodb")]
/// One stored item as a [`Record`], or a refusal.
///
/// Strict in every direction: the attribute set must be exactly [`ATTRIBUTES`],
/// the stored partition key must be the one this room addresses, and every
/// string goes through its own validating constructor.
fn read_item(
    item: &HashMap<String, AttributeValue>,
    room: &RoomId,
    expected_key: &str,
) -> Result<Record, HistoryError> {
    let seq = number(item, "seq", None)?;
    let at = Some(seq);

    if item.len() != ATTRIBUTES.len() || !ATTRIBUTES.iter().all(|name| item.contains_key(*name)) {
        return Err(HistoryError::Malformed {
            attribute: "attribute set",
            seq: at,
        });
    }

    // Compared, not parsed. An item filed under another room's key is refused
    // rather than relabelled to the room that asked for it.
    if string(item, "room", at)? != expected_key {
        return Err(HistoryError::Malformed {
            attribute: "room",
            seq: at,
        });
    }

    if seq == 0 {
        return Err(HistoryError::Malformed {
            attribute: "seq",
            seq: at,
        });
    }

    let from = HostId::try_from(string(item, "from", at)?).map_err(|_refused| {
        HistoryError::Malformed {
            attribute: "from",
            seq: at,
        }
    })?;

    let kind = kind_from_word(string(item, "kind", at)?).ok_or(HistoryError::Malformed {
        attribute: "kind",
        seq: at,
    })?;

    let raw_body = string(item, "body", at)?;
    // Size first, then the constructor. `Body::new` counts characters, which
    // means walking the string; refusing an oversized one here keeps that walk
    // bounded by a value we chose (rule 42).
    if raw_body.len() > MAX_BODY_BYTES {
        return Err(HistoryError::TooLarge {
            bytes: raw_body.len(),
        });
    }
    let body = Body::new(raw_body).map_err(|_refused| HistoryError::Malformed {
        attribute: "body",
        seq: at,
    })?;

    let posted_at = number(item, "posted_at", at)?;
    // Read back for type and presence only — an item missing it, or holding
    // one of the wrong type, is malformed the same way a bad `kind` is. The
    // filtering decision itself is made by the caller from
    // `Record::expires_at()`, recomputed from `posted_at` with the same
    // arithmetic `Dynamo::put` used to write this very attribute, rather than
    // from the value read here: the two agree under normal operation, since
    // `put` never writes anything else, and recomputing keeps this function's
    // job to "parse and validate one item" rather than "parse, validate, and
    // also decide whether it's servable."
    let _expires_at = number(item, "expires_at", at)?;

    Ok(Record {
        room: room.clone(),
        seq: RoomSeq(seq),
        from,
        kind,
        body,
        posted_at,
    })
}

#[cfg(feature = "dynamodb")]
fn string<'i>(
    item: &'i HashMap<String, AttributeValue>,
    attribute: &'static str,
    seq: Option<u64>,
) -> Result<&'i str, HistoryError> {
    item.get(attribute)
        .and_then(|value| value.as_s().ok())
        .map(String::as_str)
        .ok_or(HistoryError::Malformed { attribute, seq })
}

#[cfg(feature = "dynamodb")]
fn number(
    item: &HashMap<String, AttributeValue>,
    attribute: &'static str,
    seq: Option<u64>,
) -> Result<u64, HistoryError> {
    item.get(attribute)
        .and_then(|value| value.as_n().ok())
        .and_then(|raw| raw.parse::<u64>().ok())
        .ok_or(HistoryError::Malformed { attribute, seq })
}

/// A store that behaves like the real one without talking to anything.
///
/// Test-only, and never `pub`: it holds a room's history in memory, enforces
/// the same conditional-write refusal the real `PutItem` condition does, and
/// can be told to fail every write. The ingest path's behaviour on a write
/// failure is a property of the router, so it is tested here rather than
/// against AWS.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct Fake {
    state: std::sync::Mutex<FakeState>,
}

#[cfg(test)]
#[derive(Default)]
struct FakeState {
    /// Keyed the way the table is: partition key, then sequence number.
    items: std::collections::BTreeMap<(String, u64), Record>,
    fail_writes: bool,
    fail_reads: bool,
    /// Refuse an append of exactly this sequence number as a collision, even
    /// when nothing is stored under it. Stands in for the case the conditional
    /// write actually guards: a number the live counter believes is free but
    /// the table already holds.
    collide_at: Option<u64>,
    /// How long every append takes. Latency, not failure: the router's
    /// ordering guarantee and its write budget are both about a store that is
    /// slow rather than one that is down.
    append_delay: Duration,
    /// How long the *first* append takes, when that should differ from the
    /// rest. One slow write followed by fast ones is what makes an ordering
    /// bug deterministic rather than a race.
    first_append_delay: Option<Duration>,
    appends: u64,
    seq_reads: u64,
    /// How long every page read takes. Counted in `page_reads` before the
    /// wait, so a test can tell a read has provably started.
    page_delay: Duration,
    page_reads: u64,
}

#[cfg(test)]
impl Fake {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// A store whose every write fails, the way a DynamoDB outage does.
    pub(crate) fn failing() -> Self {
        Self {
            state: std::sync::Mutex::new(FakeState {
                fail_writes: true,
                ..FakeState::default()
            }),
        }
    }

    /// A store whose sequence read fails, so seeding cannot be done.
    pub(crate) fn unreadable() -> Self {
        Self {
            state: std::sync::Mutex::new(FakeState {
                fail_reads: true,
                ..FakeState::default()
            }),
        }
    }

    /// A store that refuses `seq` as a collision however empty it looks.
    pub(crate) fn colliding_at(seq: u64) -> Self {
        Self {
            state: std::sync::Mutex::new(FakeState {
                collide_at: Some(seq),
                ..FakeState::default()
            }),
        }
    }

    /// A store that works but takes `delay` to write, every time.
    pub(crate) fn slow(delay: Duration) -> Self {
        Self {
            state: std::sync::Mutex::new(FakeState {
                append_delay: delay,
                ..FakeState::default()
            }),
        }
    }

    /// A store whose first write takes `delay` and whose later writes are
    /// instant.
    pub(crate) fn slow_first(delay: Duration) -> Self {
        Self {
            state: std::sync::Mutex::new(FakeState {
                first_append_delay: Some(delay),
                ..FakeState::default()
            }),
        }
    }

    /// Makes every page read take `delay`, from now on.
    pub(crate) fn set_page_delay(&self, delay: Duration) {
        self.lock().page_delay = delay;
    }

    /// Page reads started, counted before any delay.
    pub(crate) fn page_reads(&self) -> u64 {
        self.lock().page_reads
    }

    /// Makes every read fail from now on, the way an unreachable table does.
    pub(crate) fn fail_reads(&self) {
        self.lock().fail_reads = true;
    }

    /// Stores `record` as if it had been written, bypassing every check.
    pub(crate) fn insert(&self, record: Record) {
        let key = (partition_key(&record.room), record.seq.0);
        self.lock().items.insert(key, record);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Pre-seeds `room` with messages 1..=`through`, as a room that has
    /// already been used and then emptied would have left behind.
    ///
    /// Posted just now, not at a fixed epoch second: XYZ-108 makes the read
    /// path filter on `expires_at`, computed from `posted_at`, against the
    /// real clock. A fixed historical timestamp would eventually — and, for a
    /// worktree checked out well after this was written, immediately — read
    /// as already expired and vanish from every page, which is a test that
    /// depends on when it runs rather than on what it is testing (worker
    /// contract rule 4).
    pub(crate) fn seeded(room: &RoomId, through: u64, from: &HostId) -> Self {
        let fake = Self::new();
        let posted_base = now_epoch().unwrap_or(0);
        {
            let key = partition_key(room);
            let mut state = fake.lock();
            for seq in 1..=through {
                state.items.insert(
                    (key.clone(), seq),
                    Record {
                        room: room.clone(),
                        seq: RoomSeq(seq),
                        from: from.clone(),
                        kind: MessageKind::Claim,
                        body: Body::new(format!("historic message {seq}")).unwrap_or_else(|_| {
                            // Unreachable: the text is non-empty and short.
                            Body::new("historic").unwrap_or_else(|_| unreachable!())
                        }),
                        posted_at: posted_base.saturating_add(seq),
                    },
                );
            }
        }
        fake
    }

    pub(crate) fn appends(&self) -> u64 {
        self.lock().appends
    }

    pub(crate) fn seq_reads(&self) -> u64 {
        self.lock().seq_reads
    }

    /// Every sequence number stored for `room`, in order.
    pub(crate) fn seqs_of(&self, room: &RoomId) -> Vec<u64> {
        let key = partition_key(room);
        self.lock()
            .items
            .keys()
            .filter(|(stored, _)| *stored == key)
            .map(|(_, seq)| *seq)
            .collect()
    }

    /// The `expires_at` stored for one message, computed the way the real
    /// store computes it.
    pub(crate) fn expires_at_of(&self, room: &RoomId, seq: u64) -> Option<u64> {
        let key = partition_key(room);
        self.lock()
            .items
            .get(&(key, seq))
            .and_then(Record::expires_at)
    }

    pub(crate) fn bodies_of(&self, room: &RoomId) -> Vec<String> {
        let key = partition_key(room);
        self.lock()
            .items
            .iter()
            .filter(|((stored, _), _)| *stored == key)
            .map(|(_, record)| record.body.to_string())
            .collect()
    }
}

#[cfg(test)]
impl RoomHistory for Fake {
    fn append<'a>(&'a self, record: &'a Record) -> BoxFuture<'a, Result<(), HistoryError>> {
        Box::pin(async move {
            // The delay is decided and the guard dropped *before* the sleep: a
            // `std::sync::MutexGuard` held across an await would make this
            // future `!Send` and could wedge the runtime.
            let delay = {
                let mut state = self.lock();
                state.appends = state.appends.saturating_add(1);
                match (state.appends, state.first_append_delay) {
                    (1, Some(first)) => first,
                    _ => state.append_delay,
                }
            };
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let mut state = self.lock();
            if state.fail_writes {
                return Err(HistoryError::Unavailable);
            }
            let key = (partition_key(&record.room), record.seq.0);
            // The real store's `attribute_not_exists` condition, in memory.
            if state.items.contains_key(&key) || state.collide_at == Some(record.seq.0) {
                return Err(HistoryError::Collision { seq: record.seq.0 });
            }
            state.items.insert(key, record.clone());
            Ok(())
        })
    }

    fn highest_seq<'a>(
        &'a self,
        room: &'a RoomId,
    ) -> BoxFuture<'a, Result<Option<u64>, HistoryError>> {
        Box::pin(async move {
            let key = partition_key(room);
            let mut state = self.lock();
            state.seq_reads = state.seq_reads.saturating_add(1);
            if state.fail_reads {
                return Err(HistoryError::Unavailable);
            }
            Ok(state
                .items
                .keys()
                .filter(|(stored, _)| *stored == key)
                .map(|(_, seq)| *seq)
                .max())
        })
    }

    fn page<'a>(
        &'a self,
        room: &'a RoomId,
        after: Option<u64>,
        limit: i32,
    ) -> BoxFuture<'a, Result<Page, HistoryError>> {
        Box::pin(async move {
            let key = partition_key(room);
            let limit = usize::try_from(limit.clamp(1, MAX_PAGE_ITEMS)).unwrap_or(1);
            // Counted, and the guard dropped, before the wait: see `append`.
            let delay = {
                let mut state = self.lock();
                state.page_reads = state.page_reads.saturating_add(1);
                state.page_delay
            };
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let state = self.lock();
            if state.fail_reads {
                return Err(HistoryError::Unavailable);
            }
            let now = now_epoch()?;
            let mut all = state
                .items
                .iter()
                .filter(|((stored, seq), _)| {
                    *stored == key && after.is_none_or(|after| *seq > after)
                })
                .map(|(_, record)| record.clone());

            // Scan up to `limit` in sequence order first, the same window the
            // real store's `Query` would return, then window it exactly as the
            // real store does: expired records skipped, the byte cap
            // applied, and `next` resuming after the last item
            // *scanned* unless the cap cut the page.
            let scanned: Vec<Record> = all.by_ref().take(limit).collect();
            let more = all.next().is_some();
            window_records(scanned.into_iter().map(Ok), more, now, MAX_PAGE_BYTES)
        })
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

    use sealmesh_wire::{ProjectId, RepoSlug, TicketId};

    use super::*;

    fn project_room() -> RoomId {
        RoomId::new(ProjectId::try_from("example-repo").unwrap(), Scope::Project)
    }

    fn host() -> HostId {
        HostId::try_from("sandbank").unwrap()
    }

    fn record(room: &RoomId, seq: u64, text: &str, posted_at: u64) -> Record {
        Record {
            room: room.clone(),
            seq: RoomSeq(seq),
            from: host(),
            kind: MessageKind::Claim,
            body: Body::new(text).unwrap(),
            posted_at,
        }
    }

    /// A record posted a minute ago: comfortably inside [`RETENTION`].
    ///
    /// Relative to the real clock, not a fixed epoch second (worker contract
    /// rule 4) — a hardcoded `posted_at` reads as already expired the moment
    /// it is more than seven days old, which for a fixture is only a matter
    /// of when the test happens to run.
    fn live_record(room: &RoomId, seq: u64, text: &str) -> Record {
        record(room, seq, text, now_epoch().unwrap().saturating_sub(60))
    }

    /// A record whose retention window closed an hour ago: exactly the
    /// "TTL deletion lagged" case XYZ-108 is about — still in the table,
    /// past its `expires_at`.
    fn expired_record(room: &RoomId, seq: u64, text: &str) -> Record {
        record(
            room,
            seq,
            text,
            now_epoch()
                .unwrap()
                .saturating_sub(RETENTION.as_secs() + 3_600),
        )
    }

    #[cfg(feature = "dynamodb")]
    fn item(pairs: &[(&str, AttributeValue)]) -> HashMap<String, AttributeValue> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect()
    }

    #[cfg(feature = "dynamodb")]
    fn good_item(key: &str) -> HashMap<String, AttributeValue> {
        item(&[
            ("room", AttributeValue::S(key.to_owned())),
            ("seq", AttributeValue::N("4".to_owned())),
            ("from", AttributeValue::S("sandbank".to_owned())),
            ("kind", AttributeValue::S("claim".to_owned())),
            ("body", AttributeValue::S("taking ENG-557".to_owned())),
            ("posted_at", AttributeValue::N("1700000000".to_owned())),
            ("expires_at", AttributeValue::N("1700604800".to_owned())),
        ])
    }

    #[cfg(feature = "dynamodb")]
    /// A raw stored item the way [`Dynamo::put`] would have written it, with
    /// `posted_at` and `seq` controllable — for [`window_page`] tests, which
    /// exercise the real production windowing logic directly rather than only
    /// through the fake store's separate reimplementation.
    fn stored_item(
        key: &str,
        seq: u64,
        posted_at: u64,
        body: &str,
    ) -> HashMap<String, AttributeValue> {
        let expires_at = posted_at.saturating_add(RETENTION.as_secs());
        item(&[
            ("room", AttributeValue::S(key.to_owned())),
            ("seq", AttributeValue::N(seq.to_string())),
            ("from", AttributeValue::S("sandbank".to_owned())),
            ("kind", AttributeValue::S("claim".to_owned())),
            ("body", AttributeValue::S(body.to_owned())),
            ("posted_at", AttributeValue::N(posted_at.to_string())),
            ("expires_at", AttributeValue::N(expires_at.to_string())),
        ])
    }

    #[test]
    fn retention_is_exactly_seven_days() {
        // The number the README's privacy promise quotes. Spelled out in
        // seconds so a change to the constant has to change this line too.
        assert_eq!(RETENTION.as_secs(), 604_800);
        assert_eq!(RETENTION, Duration::from_secs(7 * 24 * 60 * 60));
    }

    #[test]
    fn expires_at_is_exactly_seven_days_past_the_post() {
        let posted_at = 1_700_000_000;
        let record = record(&project_room(), 1, "hello", posted_at);
        assert_eq!(record.expires_at(), Some(posted_at + 604_800));
    }

    #[test]
    fn an_expiry_that_would_overflow_is_refused_rather_than_wrapped() {
        // A wrapped `expires_at` lands in the past, which the TTL reads as
        // "delete immediately" (rule 40).
        let record = record(&project_room(), 1, "hello", u64::MAX);
        assert_eq!(record.expires_at(), None);
    }

    #[test]
    fn every_scope_has_its_own_partition_key_and_none_can_forge_another() {
        let project = project_room();
        let repo = RoomId::new(
            ProjectId::try_from("example-repo").unwrap(),
            Scope::Repo(RepoSlug::try_from("example-org/example-repo").unwrap()),
        );
        let ticket = RoomId::new(
            ProjectId::try_from("example-repo").unwrap(),
            Scope::Ticket(TicketId::new("ENG", 557).unwrap()),
        );

        assert_eq!(partition_key(&project), "example-repo#project");
        assert_eq!(
            partition_key(&repo),
            "example-repo#repo#example-org/example-repo"
        );
        assert_eq!(partition_key(&ticket), "example-repo#ticket#ENG-557");

        let keys = [
            partition_key(&project),
            partition_key(&repo),
            partition_key(&ticket),
        ];
        let unique: std::collections::BTreeSet<&String> = keys.iter().collect();
        assert_eq!(unique.len(), keys.len(), "two rooms share a key");
    }

    #[test]
    fn two_projects_cannot_collide_on_one_key() {
        // `#` is in no identifier charset, so no project or repo name can
        // spell another room's key.
        let a = RoomId::new(
            ProjectId::try_from("example-repo-portal").unwrap(),
            Scope::Project,
        );
        let b = RoomId::new(ProjectId::try_from("example-repo").unwrap(), Scope::Project);
        assert_ne!(partition_key(&a), partition_key(&b));
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn the_table_name_is_required_and_never_defaulted() {
        let missing = table_from_env(Err(std::env::VarError::NotPresent))
            .unwrap_err()
            .to_string();
        assert!(missing.contains(TABLE_VAR), "{missing}");

        let blank = table_from_env(Ok("   ".to_owned()))
            .unwrap_err()
            .to_string();
        assert!(blank.contains(TABLE_VAR), "{blank}");

        assert_eq!(
            table_from_env(Ok(" dev-sealmesh-room-history ".to_owned())).unwrap(),
            "dev-sealmesh-room-history"
        );
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn a_stored_item_round_trips_through_the_strict_reader() {
        let room = project_room();
        let key = partition_key(&room);
        let parsed = read_item(&good_item(&key), &room, &key).unwrap();
        assert_eq!(parsed.seq, RoomSeq(4));
        assert_eq!(parsed.from, host());
        assert_eq!(parsed.kind, MessageKind::Claim);
        assert_eq!(parsed.body.as_str(), "taking ENG-557");
        assert_eq!(parsed.posted_at, 1_700_000_000);
        assert_eq!(parsed.room, room);
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn an_unknown_attribute_refuses_the_whole_record() {
        let room = project_room();
        let key = partition_key(&room);
        let mut hostile = good_item(&key);
        hostile.insert(
            "intent".to_owned(),
            AttributeValue::S("the prompt".to_owned()),
        );
        assert!(matches!(
            read_item(&hostile, &room, &key),
            Err(HistoryError::Malformed { .. })
        ));
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn a_missing_attribute_refuses_the_record() {
        let room = project_room();
        let key = partition_key(&room);
        for absent in ATTRIBUTES {
            let mut hostile = good_item(&key);
            hostile.remove(absent);
            assert!(
                read_item(&hostile, &room, &key).is_err(),
                "accepted an item with no {absent}"
            );
        }
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn an_item_filed_under_another_room_is_refused_not_relabelled() {
        let room = project_room();
        let key = partition_key(&room);
        let other = good_item("other#project");
        assert!(matches!(
            read_item(&other, &room, &key),
            Err(HistoryError::Malformed {
                attribute: "room",
                ..
            })
        ));
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn a_value_its_constructor_refuses_refuses_the_record() {
        let room = project_room();
        let key = partition_key(&room);

        let cases: [(&str, AttributeValue); 5] = [
            // Uppercase is not a `HostId`.
            ("from", AttributeValue::S("Sandbank".to_owned())),
            // Not one of the four kinds.
            ("kind", AttributeValue::S("urgent".to_owned())),
            // `Body` refuses empty.
            ("body", AttributeValue::S(String::new())),
            // A string where a number belongs.
            ("seq", AttributeValue::S("4".to_owned())),
            // Zero is not a sequence number.
            ("seq", AttributeValue::N("0".to_owned())),
        ];
        for (attribute, value) in cases {
            let mut hostile = good_item(&key);
            hostile.insert(attribute.to_owned(), value);
            assert!(
                read_item(&hostile, &room, &key).is_err(),
                "accepted a bad {attribute}"
            );
        }
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn an_oversized_body_is_refused_on_size_before_it_is_parsed() {
        let room = project_room();
        let key = partition_key(&room);
        let mut hostile = good_item(&key);
        let huge = "x".repeat(MAX_BODY_BYTES + 1);
        hostile.insert("body".to_owned(), AttributeValue::S(huge));
        assert!(matches!(
            read_item(&hostile, &room, &key),
            Err(HistoryError::TooLarge { .. })
        ));
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn no_error_carries_a_value_that_came_from_the_table() {
        // Rules 14 and 26. The secret is in the body and the host; the error
        // may name the attribute and the sequence number and nothing else.
        let room = project_room();
        let key = partition_key(&room);
        let mut hostile = good_item(&key);
        hostile.insert(
            "body".to_owned(),
            AttributeValue::S("x".repeat(MAX_BODY_BYTES + 1)),
        );
        let rendered = read_item(&hostile, &room, &key).unwrap_err().to_string();
        assert!(!rendered.contains('x'), "{rendered}");

        let mut forged = good_item(&key);
        forged.insert(
            "from".to_owned(),
            AttributeValue::S("PASTED-SECRET".to_owned()),
        );
        let rendered = read_item(&forged, &room, &key).unwrap_err().to_string();
        assert!(!rendered.contains("PASTED"), "{rendered}");
    }

    // ---- is_expired and window_page: XYZ-108's production windowing logic,
    // exercised directly rather than only through the fake store's separate
    // reimplementation ----

    #[test]
    fn is_expired_at_the_exact_boundary_and_on_overflow() {
        let room = project_room();
        let now = 1_700_000_000u64;
        // Exactly at expiry: expires_at == now counts as expired, one second
        // stricter than the TTL's own "less than now" eligibility.
        let at_boundary = record(&room, 1, "boundary", now - RETENTION.as_secs());
        assert!(is_expired(&at_boundary, now));
        // One second inside retention: not expired.
        let just_inside = record(&room, 2, "just inside", now - RETENTION.as_secs() + 1);
        assert!(!is_expired(&just_inside, now));
        // posted_at near u64::MAX overflows expires_at() to None, which
        // is_expired treats as expired — refusing to serve is the safe
        // direction when the bound cannot even be computed.
        let overflow = record(&room, 3, "overflow", u64::MAX);
        assert!(is_expired(&overflow, now));
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn window_page_skips_an_expired_item_among_live_ones() {
        let room = project_room();
        let key = partition_key(&room);
        let now = now_epoch().unwrap();
        let items = vec![
            stored_item(
                &key,
                1,
                now.saturating_sub(RETENTION.as_secs() + 3_600),
                "old",
            ),
            stored_item(&key, 2, now.saturating_sub(60), "live"),
        ];
        let page = window_page(&items, &room, &key, false, now, MAX_PAGE_BYTES).unwrap();
        assert_eq!(
            page.messages
                .iter()
                .map(|record| record.seq.0)
                .collect::<Vec<_>>(),
            vec![2],
            "an expired item reached the caller"
        );
        assert_eq!(page.next, None);
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn window_page_with_more_advances_past_a_fully_expired_window() {
        let room = project_room();
        let key = partition_key(&room);
        let now = now_epoch().unwrap();
        let expired_at = now.saturating_sub(RETENTION.as_secs() + 3_600);
        let items = vec![
            stored_item(&key, 1, expired_at, "old1"),
            stored_item(&key, 2, expired_at, "old2"),
        ];
        let page = window_page(&items, &room, &key, true, now, MAX_PAGE_BYTES).unwrap();
        assert!(
            page.messages.is_empty(),
            "the whole window was expired, nothing should be kept"
        );
        assert_eq!(
            page.next,
            Some(2),
            "a filtered-empty window must still advance from what it scanned"
        );
    }

    #[cfg(feature = "dynamodb")]
    #[test]
    fn window_page_byte_cap_ignores_expired_items_on_either_side_of_the_trip() {
        let room = project_room();
        let key = partition_key(&room);
        let now = now_epoch().unwrap();
        let live_at = now.saturating_sub(60);
        let expired_at = now.saturating_sub(RETENTION.as_secs() + 3_600);
        let items = vec![
            // Expired, before anything live: never counted or kept.
            stored_item(&key, 1, expired_at, "expired-before"),
            // Live, 5 bytes: kept, bytes = 5.
            stored_item(&key, 2, live_at, "abcde"),
            // Expired, between two live items, with a body far bigger than
            // the injected cap: if expired items were wrongly counted toward
            // the byte cap, this would trip it here instead of at item 5.
            stored_item(&key, 3, expired_at, &"x".repeat(200)),
            // Live, 5 bytes: kept, bytes = 10 (<= cap 12).
            stored_item(&key, 4, live_at, "fghij"),
            // Live, 5 bytes: bytes would become 15 (> cap 12) — trips the
            // byte ceiling and is not itself kept.
            stored_item(&key, 5, live_at, "klmno"),
        ];
        let page = window_page(&items, &room, &key, false, now, 12).unwrap();
        assert_eq!(
            page.messages
                .iter()
                .map(|record| record.seq.0)
                .collect::<Vec<_>>(),
            vec![2, 4],
            "expired items on either side of the trip point changed what was kept"
        );
        assert_eq!(
            page.next,
            Some(4),
            "resume should be the last item actually kept, not an expired one"
        );
    }

    #[tokio::test]
    async fn the_fake_refuses_a_collision_the_way_the_conditional_write_does() {
        let room = project_room();
        let store = Fake::new();
        assert!(store.append(&record(&room, 1, "first", 10)).await.is_ok());
        assert_eq!(
            store.append(&record(&room, 1, "second", 11)).await,
            Err(HistoryError::Collision { seq: 1 })
        );
        // The first message is still the stored one: nothing was overwritten.
        assert_eq!(store.bodies_of(&room), vec!["first".to_owned()]);
    }

    #[tokio::test]
    async fn history_reads_back_in_sequence_order_across_pages() {
        let room = project_room();
        let store = Fake::seeded(&room, 5, &host());

        let first = store.page(&room, None, 2).await.unwrap();
        assert_eq!(
            first
                .messages
                .iter()
                .map(|record| record.seq.0)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(first.next, Some(2));

        let second = store.page(&room, first.next, 2).await.unwrap();
        assert_eq!(
            second
                .messages
                .iter()
                .map(|record| record.seq.0)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );

        let whole = backfill(&store, &room, None).await.unwrap();
        assert_eq!(
            whole
                .messages
                .iter()
                .map(|record| record.seq.0)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(whole.next, None);
    }

    #[tokio::test]
    async fn a_backfill_reads_at_most_the_page_cap_and_says_where_to_resume() {
        let room = project_room();
        // More history than MAX_PAGES pages of MAX_PAGE_ITEMS can hold.
        let total = u64::try_from(MAX_PAGES).unwrap() * u64::try_from(MAX_PAGE_ITEMS).unwrap() + 10;
        let store = Fake::seeded(&room, total, &host());

        let page = backfill(&store, &room, None).await.unwrap();
        assert_eq!(
            page.messages.len(),
            MAX_PAGES * usize::try_from(MAX_PAGE_ITEMS).unwrap(),
            "a backfill read past its page cap"
        );
        assert_eq!(page.next, Some(1024));
        // And it resumes rather than losing the tail.
        let rest = backfill(&store, &room, page.next).await.unwrap();
        assert_eq!(rest.messages.len(), 10);
        assert_eq!(rest.next, None);
    }

    #[tokio::test]
    async fn a_page_is_capped_in_count_however_much_is_asked_for() {
        let room = project_room();
        let store = Fake::seeded(&room, 200, &host());
        let page = store.page(&room, None, i32::MAX).await.unwrap();
        assert_eq!(
            page.messages.len(),
            usize::try_from(MAX_PAGE_ITEMS).unwrap()
        );
    }

    /// `through` messages in `room`, each a body of the largest size a
    /// stored body may be. Posted a minute ago by the real clock, so the
    /// expiry filter keeps every one of them and the byte caps are
    /// what cut the read.
    fn widest(room: &RoomId, through: u64) -> Fake {
        let store = Fake::new();
        let text = "😀".repeat(MAX_BODY_CHARS);
        assert_eq!(text.len(), MAX_BODY_BYTES);
        let posted = now_epoch().unwrap().saturating_sub(60);
        for seq in 1..=through {
            store.insert(record(room, seq, &text, posted));
        }
        store
    }

    #[tokio::test]
    async fn a_page_of_large_bodies_is_cut_by_its_byte_cap_before_its_count_cap() {
        // XYZ-96: the byte cap used to sit above what the count cap allowed,
        // so it could never fire. It is now the tighter bound for large
        // bodies, and the cut says where to resume.
        let room = project_room();
        let store = widest(&room, u64::try_from(MAX_PAGE_ITEMS).unwrap());
        let fit = MAX_PAGE_BYTES / MAX_BODY_BYTES;
        assert!(fit < usize::try_from(MAX_PAGE_ITEMS).unwrap());

        let page = store.page(&room, None, MAX_PAGE_ITEMS).await.unwrap();
        assert_eq!(
            page.messages.len(),
            fit,
            "a page kept more than its byte cap"
        );
        assert_eq!(page.next, Some(u64::try_from(fit).unwrap()));
        let kept: usize = page.messages.iter().map(|r| r.body.as_str().len()).sum();
        assert!(kept <= MAX_PAGE_BYTES, "{kept} bytes kept");
    }

    #[tokio::test]
    async fn a_backfill_keeps_at_most_its_byte_cap_across_pages_and_says_where_to_resume() {
        // XYZ-96: sixteen pages each under the page cap used to add up to
        // 4 MiB of bodies in one call, with nothing bounding the total.
        let room = project_room();
        let total = 100;
        let store = widest(&room, total);
        let fit = MAX_BACKFILL_BYTES / MAX_BODY_BYTES;

        let read = backfill(&store, &room, None).await.unwrap();
        assert_eq!(
            read.messages.len(),
            fit,
            "a backfill kept more than its byte cap"
        );
        let kept: usize = read.messages.iter().map(|r| r.body.as_str().len()).sum();
        assert!(kept <= MAX_BACKFILL_BYTES, "{kept} bytes kept");
        assert_eq!(read.next, Some(u64::try_from(fit).unwrap()));

        // Resuming picks up at the first message left out, and reaches the end.
        let mut seen = read.messages.len();
        let mut next = read.next;
        while let Some(after) = next {
            let more = backfill(&store, &room, Some(after)).await.unwrap();
            assert_eq!(more.messages.first().unwrap().seq.0, after + 1);
            seen += more.messages.len();
            next = more.next;
        }
        assert_eq!(seen, usize::try_from(total).unwrap());
    }

    #[tokio::test]
    async fn one_rooms_history_is_never_served_to_another() {
        let example_repo = project_room();
        let portal = RoomId::new(
            ProjectId::try_from("example-repo-portal").unwrap(),
            Scope::Project,
        );
        let store = Fake::seeded(&example_repo, 3, &host());
        assert!(
            store
                .page(&portal, None, 10)
                .await
                .unwrap()
                .messages
                .is_empty()
        );
        assert_eq!(store.highest_seq(&portal).await.unwrap(), None);
        assert_eq!(store.highest_seq(&example_repo).await.unwrap(), Some(3));
    }

    // ---- XYZ-108: never serve a message past its 7-day expiry ----

    #[cfg(feature = "dynamodb")]
    #[tokio::test]
    async fn an_item_past_its_expiry_but_not_yet_deleted_is_never_returned() {
        // Simulates exactly what DynamoDB's TTL leaves behind while it lags:
        // an item still in the table, past its `expires_at`. `insert` writes
        // straight into the fake's store, bypassing any check `append` would
        // make, the same way a real table holds it until the TTL sweep gets
        // to it.
        let room = project_room();
        let store = Fake::new();
        store.insert(expired_record(&room, 1, "eight days old"));
        store.insert(live_record(&room, 2, "just now"));

        let page = store.page(&room, None, MAX_PAGE_ITEMS).await.unwrap();
        assert_eq!(
            page.messages
                .iter()
                .map(|record| record.seq.0)
                .collect::<Vec<_>>(),
            vec![2],
            "an expired-but-undeleted item was served"
        );

        let read = backfill(&store, &room, None).await.unwrap();
        assert_eq!(
            read.messages
                .iter()
                .map(|record| record.seq.0)
                .collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[tokio::test]
    async fn a_page_that_filters_to_empty_still_advances_and_reports_where_to_resume() {
        let room = project_room();
        let store = Fake::new();
        // A full page's worth of expired items, followed by live ones. A
        // single `page()` call scans exactly MAX_PAGE_ITEMS in sequence
        // order, so this window is entirely expired.
        let items = usize::try_from(MAX_PAGE_ITEMS).unwrap();
        for seq in 1..=items {
            store.insert(expired_record(&room, u64::try_from(seq).unwrap(), "old"));
        }
        let live_seq = u64::try_from(items).unwrap() + 1;
        store.insert(live_record(&room, live_seq, "still here"));

        let first = store.page(&room, None, MAX_PAGE_ITEMS).await.unwrap();
        assert!(
            first.messages.is_empty(),
            "the whole window was expired, nothing should be kept"
        );
        assert_eq!(
            first.next,
            Some(u64::try_from(items).unwrap()),
            "a filtered-empty page must still advance rather than stalling"
        );

        let second = store.page(&room, first.next, MAX_PAGE_ITEMS).await.unwrap();
        assert_eq!(
            second
                .messages
                .iter()
                .map(|record| record.seq.0)
                .collect::<Vec<_>>(),
            vec![live_seq]
        );
        assert_eq!(second.next, None);
    }

    #[tokio::test]
    async fn a_backfill_advances_past_a_fully_expired_page_and_cannot_loop() {
        // The end-to-end proof that `backfill_until`'s pagination loop makes
        // progress on a filtered-empty page rather than reading it as the end
        // of the room's history — and that this is still bounded, since the
        // test completing at all is the proof no loop ran forever.
        let room = project_room();
        let store = Fake::new();
        let items = usize::try_from(MAX_PAGE_ITEMS).unwrap();
        for seq in 1..=items {
            store.insert(expired_record(&room, u64::try_from(seq).unwrap(), "old"));
        }
        let live_seq = u64::try_from(items).unwrap() + 1;
        store.insert(live_record(&room, live_seq, "still here"));

        let read = backfill(&store, &room, None).await.unwrap();
        assert_eq!(
            read.messages
                .iter()
                .map(|record| record.seq.0)
                .collect::<Vec<_>>(),
            vec![live_seq],
            "backfill did not advance past the fully-expired page"
        );
        assert_eq!(read.next, None);
        // Exactly two page reads: the expired window, then the live tail.
        assert_eq!(store.page_reads(), 2);
    }

    #[tokio::test]
    async fn backfill_until_refuses_rather_than_looping_or_lying_about_the_end() {
        // Defense in depth: no `page()` in this file can produce a
        // non-advancing `next` today, since every one derives it from a
        // `seq > after` key condition (see `backfill_until`'s own comment on
        // the guard this proves). But `backfill_until` does not take that on
        // faith — a `RoomHistory` whose `next` gets stuck at the same value
        // is refused rather than retried forever or read as a quiet room's
        // genuine end: treating a store defect as "no more history" would
        // silently truncate what a caller sees, so it surfaces as
        // `HistoryError::Unavailable` instead, bounded well short of
        // `MAX_PAGES` rather than only by it.
        struct Stuck;
        impl RoomHistory for Stuck {
            fn append<'a>(
                &'a self,
                _record: &'a Record,
            ) -> BoxFuture<'a, Result<(), HistoryError>> {
                Box::pin(async { Err(HistoryError::Unavailable) })
            }
            fn highest_seq<'a>(
                &'a self,
                _room: &'a RoomId,
            ) -> BoxFuture<'a, Result<Option<u64>, HistoryError>> {
                Box::pin(async { Ok(None) })
            }
            fn page<'a>(
                &'a self,
                _room: &'a RoomId,
                _after: Option<u64>,
                _limit: i32,
            ) -> BoxFuture<'a, Result<Page, HistoryError>> {
                // Always claims there is more, at the same cursor, with
                // nothing to show for it: the shape a broken store's "more"
                // signal could take.
                Box::pin(async {
                    Ok(Page {
                        messages: Vec::new(),
                        next: Some(1),
                    })
                })
            }
        }

        let room = project_room();
        let read = backfill(&Stuck, &room, None).await;
        assert_eq!(
            read,
            Err(HistoryError::Unavailable),
            "a cursor that stopped advancing should be refused, not read as the end or looped"
        );
    }

    /// The behavior every [`RoomHistory`] store shares, run against each one.
    ///
    /// The SQLite run is ignored until `SqliteHistory`'s store methods are
    /// written (sealmesh-r1 step 5, by hand). Remove its `ignore` then; every
    /// test here must pass on both stores.
    mod contract {
        use super::*;

        fn ticket_room() -> RoomId {
            RoomId::new(
                ProjectId::try_from("example-repo").unwrap(),
                Scope::Ticket(TicketId::new("ENG", 1).unwrap()),
            )
        }

        fn seqs(page: &Page) -> Vec<u64> {
            page.messages.iter().map(|record| record.seq.0).collect()
        }

        macro_rules! contract_tests {
            ($name:ident, $make:expr $(, #[$attr:meta])?) => {
                mod $name {
                    use super::*;

                    fn store() -> impl RoomHistory {
                        $make
                    }

                    #[tokio::test]
                    $(#[$attr])?
                    async fn an_empty_room_has_no_history() {
                        let store = store();
                        let room = project_room();
                        assert_eq!(store.highest_seq(&room).await.unwrap(), None);
                        assert_eq!(store.page(&room, None, MAX_PAGE_ITEMS).await.unwrap(), Page::default());
                    }

                    #[tokio::test]
                    $(#[$attr])?
                    async fn appended_messages_page_back_in_sequence_order() {
                        let store = store();
                        let room = project_room();
                        let written: Vec<Record> =
                            [3, 1, 2].iter().map(|&seq| live_record(&room, seq, &format!("m{seq}"))).collect();
                        for record in &written {
                            store.append(record).await.unwrap();
                        }
                        let page = store.page(&room, None, MAX_PAGE_ITEMS).await.unwrap();
                        assert_eq!(seqs(&page), [1, 2, 3]);
                        assert_eq!(page.next, None);
                        assert_eq!(page.messages[0], written[1], "a record reads back exactly as written");
                        assert_eq!(store.highest_seq(&room).await.unwrap(), Some(3));
                    }

                    #[tokio::test]
                    $(#[$attr])?
                    async fn a_used_seq_is_refused_and_the_first_message_kept() {
                        let store = store();
                        let room = project_room();
                        store.append(&live_record(&room, 1, "first")).await.unwrap();
                        assert_eq!(
                            store.append(&live_record(&room, 1, "second")).await,
                            Err(HistoryError::Collision { seq: 1 })
                        );
                        let page = store.page(&room, None, MAX_PAGE_ITEMS).await.unwrap();
                        assert_eq!(page.messages.len(), 1);
                        assert_eq!(page.messages[0].body.as_str(), "first");
                    }

                    #[tokio::test]
                    $(#[$attr])?
                    async fn rooms_never_see_each_others_messages() {
                        let store = store();
                        let (project, ticket) = (project_room(), ticket_room());
                        store.append(&live_record(&project, 1, "project")).await.unwrap();
                        store.append(&live_record(&ticket, 1, "ticket")).await.unwrap();
                        store.append(&live_record(&ticket, 2, "ticket again")).await.unwrap();
                        assert_eq!(seqs(&store.page(&project, None, MAX_PAGE_ITEMS).await.unwrap()), [1]);
                        assert_eq!(store.highest_seq(&project).await.unwrap(), Some(1));
                        assert_eq!(store.highest_seq(&ticket).await.unwrap(), Some(2));
                    }

                    #[tokio::test]
                    $(#[$attr])?
                    async fn a_page_resumes_after_its_cursor_and_says_when_there_is_more() {
                        let store = store();
                        let room = project_room();
                        for seq in 1..=5 {
                            store.append(&live_record(&room, seq, "m")).await.unwrap();
                        }
                        let first = store.page(&room, None, 2).await.unwrap();
                        assert_eq!((seqs(&first), first.next), (vec![1, 2], Some(2)));
                        let second = store.page(&room, Some(2), 2).await.unwrap();
                        assert_eq!((seqs(&second), second.next), (vec![3, 4], Some(4)));
                        let last = store.page(&room, Some(4), 2).await.unwrap();
                        assert_eq!((seqs(&last), last.next), (vec![5], None));
                    }

                    #[tokio::test]
                    $(#[$attr])?
                    async fn the_page_limit_is_clamped_to_one_through_max_page_items() {
                        let store = store();
                        let room = project_room();
                        for seq in 1..=70 {
                            store.append(&live_record(&room, seq, "m")).await.unwrap();
                        }
                        assert_eq!(store.page(&room, None, 0).await.unwrap().messages.len(), 1);
                        let capped = store.page(&room, None, 1_000).await.unwrap();
                        assert_eq!(capped.messages.len(), usize::try_from(MAX_PAGE_ITEMS).unwrap());
                        assert_eq!(capped.next, Some(64));
                    }

                    #[tokio::test]
                    $(#[$attr])?
                    async fn an_expired_message_is_never_served_but_still_counts_for_highest_seq() {
                        let store = store();
                        let room = project_room();
                        store.append(&live_record(&room, 1, "live")).await.unwrap();
                        store.append(&expired_record(&room, 2, "expired")).await.unwrap();
                        let page = store.page(&room, None, MAX_PAGE_ITEMS).await.unwrap();
                        assert_eq!(seqs(&page), [1]);
                        assert_eq!(store.highest_seq(&room).await.unwrap(), Some(2));
                    }

                    #[tokio::test]
                    $(#[$attr])?
                    async fn a_page_stops_before_the_byte_cap_and_resumes_from_there() {
                        let store = store();
                        let room = project_room();
                        // 12,000 bytes each: the page cap trips well before 64 messages.
                        let wide = "€".repeat(MAX_BODY_CHARS);
                        for seq in 1..=64 {
                            store.append(&live_record(&room, seq, &wide)).await.unwrap();
                        }
                        let page = store.page(&room, None, MAX_PAGE_ITEMS).await.unwrap();
                        let bytes: usize = page.messages.iter().map(|r| r.body.as_str().len()).sum();
                        assert!(!page.messages.is_empty() && page.messages.len() < 64);
                        assert!(bytes <= MAX_PAGE_BYTES, "{bytes} bytes");
                        assert_eq!(page.next, page.messages.last().map(|r| r.seq.0));
                    }

                    #[tokio::test]
                    $(#[$attr])?
                    async fn backfill_reads_a_long_room_end_to_end() {
                        let store = store();
                        let room = project_room();
                        for seq in 1..=150 {
                            store.append(&live_record(&room, seq, "m")).await.unwrap();
                        }
                        let page = backfill(&store, &room, None).await.unwrap();
                        assert_eq!(seqs(&page), (1..=150).collect::<Vec<_>>());
                        assert_eq!(page.next, None);
                    }
                }
            };
        }

        contract_tests!(fake, Fake::new());
        contract_tests!(
            sqlite,
            crate::history::sqlite::SqliteHistory::open_in_memory().unwrap()
        );
    }
}
