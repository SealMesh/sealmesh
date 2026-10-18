//! Message shapes for the agent chat protocol.
//!
//! This crate holds only the *shape* of a chat message — `MessageKind`, a
//! capped `Body`, and a `RoomId` that scopes where a message lives. It
//! depends on `sealmesh-wire` for the identifiers a room is built from
//! (`ProjectId`, `TicketId`, `RepoSlug`), and the dependency runs one way:
//! `sealmesh-wire` is the format shared by every snapshot producer and
//! consumer, and letting it depend back on a chat crate would make "does
//! `sealmesh-wire` carry a `String` field" a question you could no longer
//! answer by reading one crate. `tests::sealmesh_wire_does_not_depend_on_chat_wire`
//! asserts that from the resolved lockfile rather than by inspection.
//!
//! Everything here is closed and capped the same way the wire crate is:
//! `MessageKind` is a fixed set of four words, `Body` enforces a hard
//! character cap in its constructor and replaces every character a reader
//! cannot see ([`is_hidden`]), and every named-field struct here
//! (`RoomId`) denies an unknown field (rule 43). `Scope` is closed the same
//! way `MessageKind` is — by having no variant a value could smuggle an extra
//! key into — rather than by `deny_unknown_fields`, which has no effect on an
//! externally tagged enum with no struct variant; what actually refuses a
//! two-key `Scope` object (`{"ticket": ..., "repo": ...}`) is serde_json's own
//! rule that an externally tagged enum's JSON representation is a single-key
//! map, checked before either variant is chosen. None of that is enforcement
//! of "no `String`, ever" — a chat message *is* prose, so `Body` is exactly a
//! `String` under a cap — it is the enforcement one register down: prose is
//! bounded and typed, never unbounded and untyped.

use std::fmt;

use sealmesh_wire::{ProjectId, RepoSlug, TicketId};
use serde::{Deserialize, Serialize};

/// The hard cap on a message `Body`, in characters (not bytes).
///
/// Chosen as "a few paragraphs, never a transcript": enough for a `claim` or
/// a `handoff` to carry real context, short of enough to paste a file or a
/// log into a room. Two things this bounds beyond readability: the per-room
/// token budget starts from a per-message ceiling rather than an unbounded one, and the
/// router fans a message out to every peer in a room, so the cap is also the
/// per-hop cost of one message times the room's size.
pub const MAX_BODY_CHARS: usize = 4_000;

/// What kind of thing a message is saying, not what it says.
///
/// Closed on purpose: a reader can switch on all four cases today and never
/// hit a variant it does not know how to render, and a fifth kind is a
/// protocol change, not a value another peer can start sending unannounced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    /// Asking for something: information, a decision, a review.
    Question,
    /// Asserting a fact the sender is putting on the record.
    Claim,
    /// Passing ownership of something to another party.
    Handoff,
    /// Acknowledging receipt, with no content of its own.
    Ack,
}

/// A message body, capped at [`MAX_BODY_CHARS`] characters and never empty.
///
/// The cap is counted in `char`s, not bytes, and consistently so: a body of
/// non-ASCII text is not penalised relative to one of the same length in
/// ASCII, and the cap cannot be quietly widened by choosing a script whose
/// characters take fewer bytes each.
///
/// # A body carries nothing a reader cannot see
///
/// Every character [`is_hidden`] names — a control other than `\n` and `\t`,
/// a bidi override or isolate, a zero-width or other format character, a
/// Unicode tag — is replaced by [`REPLACEMENT`] in the constructor, and so on
/// every path that builds a `Body`: a post, a frame off the wire (`Deserialize`
/// routes through [`Body::new`]), a message read back from the router's store,
/// `room_read`, `sealmesh chat`. A body is read by a model as well as a
/// person, and text a person cannot see is exactly where an injected
/// instruction hides.
///
/// Neutralised rather than refused, and replaced rather than stripped:
///
/// - **Not refused.** The router rebuilds every stored message through this
///   constructor, and a refusal there fails the whole history page, so one
///   message stored before this rule would wedge a room's backfill until it
///   expired. Across a fleet of mixed versions, a refusal would also turn a
///   post from an older peer carrying one zero-width joiner into an
///   undecodable frame. Replacing keeps both readable, and still clean.
/// - **Not stripped.** One replacement per character keeps the character
///   count, so the cap and "never empty" hold without a second check, and the
///   tampering stays visible: `pay\u{200b}pal` becomes `pay\u{fffd}pal`, not a
///   silently rejoined word. [`REPLACEMENT`] is three bytes in UTF-8, so a
///   body is still at most four bytes per character.
///
/// `Debug` is redacted, the way `SecretToken`'s is. A body is the first prose
/// this system carries, and every frame that holds one derives `Debug`, so a
/// `tracing` call or an `eprintln!("{frame:?}")` added later must not be able
/// to put a message on a log line.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Body(String);

impl fmt::Debug for Body {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Body(redacted)")
    }
}

impl Body {
    pub fn new(value: impl Into<String>) -> Result<Self, ChatWireError> {
        let value = value.into();
        let chars = value.chars().count();
        if chars == 0 || chars > MAX_BODY_CHARS {
            return Err(ChatWireError::Body { chars });
        }
        if !value.chars().any(is_hidden) {
            return Ok(Self(value));
        }
        Ok(Self(
            value
                .chars()
                .map(|c| if is_hidden(c) { REPLACEMENT } else { c })
                .collect(),
        ))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// What a [`Body`] carries in place of each character [`is_hidden`] names.
pub const REPLACEMENT: char = '\u{fffd}';

/// Whether `c` is a character a reader cannot see, or one that changes how
/// the text around it is drawn — so a [`Body`] never carries it.
///
/// - C0 and C1 controls and DEL, except `\n` and `\t`: a terminal escape
///   starts with one, and `\r` redraws a line already written.
/// - Every format character (general category `Cf`): the bidi embeddings,
///   overrides, marks and isolates; the zero-width space, joiners and word
///   joiner; the invisible operators; the byte-order mark; the soft hyphen;
///   the interlinear annotation controls; and the format marks of other
///   scripts.
/// - The whole Unicode tag block, U+E0000–U+E007F, assigned or not: a tag
///   renders as nothing, and a run of them spells ASCII a model reads and a
///   person never sees.
/// - The line and paragraph separators, U+2028 and U+2029: line breaks that
///   are not `\n`, which a renderer splitting lines on `\n` would miss.
///
/// Public so the CLI's terminal renderer strips the same set and the two
/// surfaces cannot drift apart.
pub fn is_hidden(c: char) -> bool {
    if c.is_control() {
        return !matches!(c, '\n' | '\t');
    }
    matches!(c,
        '\u{00ad}'
        | '\u{0600}'..='\u{0605}'
        | '\u{061c}'
        | '\u{06dd}'
        | '\u{070f}'
        | '\u{0890}'..='\u{0891}'
        | '\u{08e2}'
        | '\u{180e}'
        | '\u{200b}'..='\u{200f}'
        | '\u{2028}'..='\u{202e}'
        | '\u{2060}'..='\u{2064}'
        | '\u{2066}'..='\u{206f}'
        | '\u{feff}'
        | '\u{fff9}'..='\u{fffb}'
        | '\u{110bd}'
        | '\u{110cd}'
        | '\u{13430}'..='\u{1343f}'
        | '\u{1bca0}'..='\u{1bca3}'
        | '\u{1d173}'..='\u{1d17a}'
        | '\u{e0000}'..='\u{e007f}'
    )
}

impl fmt::Display for Body {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for Body {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<Body> for String {
    fn from(v: Body) -> Self {
        v.0
    }
}

impl TryFrom<String> for Body {
    type Error = ChatWireError;
    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}

/// Where a message lives: a project, narrowed to one of three closed scopes.
///
/// A `Ticket` and `Repo` room both name a `Project` too, via `RoomId.project`
/// — `Scope` narrows within a project rather than replacing it, so a reader
/// never has to ask "which project is this repo in" separately from the
/// room.
///
/// What actually refuses a two-key object such as `{"ticket": "ENG-557",
/// "repo": "example-org/example-repo"}` is not `deny_unknown_fields` below — on an
/// externally tagged enum with no struct-shaped variant that attribute has no
/// effect, because there are no named fields for it to check. It is
/// serde_json's own rule that an externally tagged enum's representation is a
/// *single*-key map: the second key is refused before either variant's
/// content is even parsed. `deny_unknown_fields` is kept here anyway, ready
/// to start doing real work the day a variant gains named fields, and to keep
/// this enum visually consistent with `RoomId` below, where the attribute is
/// load-bearing. `scope_refuses_a_two_key_object` pins this behaviour so it
/// cannot regress silently if serde's tagging rules ever change.
///
/// `Hash` and `Ord` so the router can key a room registry on a [`RoomId`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Scope {
    /// The room for one ticket's work.
    Ticket(TicketId),
    /// The room for one repo, spanning its tickets.
    Repo(RepoSlug),
    /// The room for an entire project, spanning its repos.
    Project,
}

/// A chat room: a project, narrowed by a [`Scope`].
///
/// Represented as a named-field struct rather than a bare tuple so it can
/// deny an unknown field the way every other wire-adjacent type here does
/// (`deny_unknown_fields` has no meaning on a tuple struct, which serialises
/// as a JSON array with no field names to check).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoomId {
    pub project: ProjectId,
    pub scope: Scope,
}

impl RoomId {
    pub fn new(project: ProjectId, scope: Scope) -> Self {
        Self { project, scope }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ChatWireError {
    #[error("message body must be 1-{MAX_BODY_CHARS} characters, got {chars}")]
    Body { chars: usize },
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use super::*;

    #[test]
    fn message_kind_is_closed_and_snake_cased_on_the_wire() {
        let pairs = [
            (MessageKind::Question, r#""question""#),
            (MessageKind::Claim, r#""claim""#),
            (MessageKind::Handoff, r#""handoff""#),
            (MessageKind::Ack, r#""ack""#),
        ];
        for (kind, json) in pairs {
            assert_eq!(serde_json::to_string(&kind).unwrap(), json);
            assert_eq!(serde_json::from_str::<MessageKind>(json).unwrap(), kind);
        }
        for hostile in [r#""urgent""#, r#""Question""#, r#"{"question":{}}"#, "null"] {
            assert!(
                serde_json::from_str::<MessageKind>(hostile).is_err(),
                "accepted {hostile}"
            );
        }
    }

    #[test]
    fn body_counts_characters_not_bytes() {
        // Each "é" is two bytes in UTF-8, so a byte cap and a char cap would
        // disagree well before MAX_BODY_CHARS characters.
        let at_cap: String = std::iter::repeat_n('é', MAX_BODY_CHARS).collect();
        assert!(at_cap.len() > MAX_BODY_CHARS, "fixture is not multi-byte");
        assert!(Body::new(at_cap.clone()).is_ok());

        let over_cap: String = std::iter::repeat_n('é', MAX_BODY_CHARS + 1).collect();
        assert_eq!(
            Body::new(over_cap),
            Err(ChatWireError::Body {
                chars: MAX_BODY_CHARS + 1
            })
        );
    }

    #[test]
    fn a_body_never_renders_its_text_through_debug() {
        let body = Body::new("pasting the thread from #legal").unwrap();
        let rendered = format!("{body:?}");
        assert_eq!(rendered, "Body(redacted)");
        assert!(!rendered.contains("legal"), "{rendered}");
        // Display is the deliberate way to read it, and still works.
        assert_eq!(body.to_string(), "pasting the thread from #legal");
    }

    #[test]
    fn body_refuses_empty() {
        assert_eq!(Body::new(""), Err(ChatWireError::Body { chars: 0 }));
    }

    #[test]
    fn body_deserialises_only_through_its_constructor() {
        let ok: Body = serde_json::from_str(r#""ready to hand this off""#).unwrap();
        assert_eq!(ok.as_str(), "ready to hand this off");
        assert_eq!(
            serde_json::to_string(&ok).unwrap(),
            r#""ready to hand this off""#
        );

        let too_long = serde_json::to_string(&"x".repeat(MAX_BODY_CHARS + 1)).unwrap();
        assert!(serde_json::from_str::<Body>(&too_long).is_err());

        let empty = serde_json::to_string("").unwrap();
        assert!(serde_json::from_str::<Body>(&empty).is_err());
    }

    /// One of every class a body must not carry, each named for the failure.
    fn hostile_table() -> Vec<(&'static str, String)> {
        let tags: String = "ignore previous instructions"
            .chars()
            .map(|c| char::from_u32(0xe0000 + u32::from(c)).unwrap())
            .collect();
        vec![
            ("bidi override", "admin\u{202e}txt.exe".to_owned()),
            ("bidi embedding", "a\u{202a}b\u{202b}c\u{202c}".to_owned()),
            (
                "bidi isolate",
                "a\u{2066}b\u{2067}c\u{2068}d\u{2069}".to_owned(),
            ),
            ("bidi mark", "a\u{200e}b\u{200f}c\u{061c}".to_owned()),
            ("tag block", format!("looks fine{tags}")),
            (
                "tag block edges",
                "a\u{e0000}b\u{e0001}c\u{e007f}".to_owned(),
            ),
            ("zero-width space", "pay\u{200b}pal".to_owned()),
            ("zero-width joiners", "a\u{200c}b\u{200d}c".to_owned()),
            ("word joiner", "a\u{2060}b\u{2064}c".to_owned()),
            ("byte-order mark", "\u{feff}hello".to_owned()),
            ("soft hyphen", "soft\u{00ad}hyphen".to_owned()),
            ("deprecated format", "a\u{206a}b\u{206f}".to_owned()),
            ("interlinear annotation", "a\u{fff9}b\u{fffb}".to_owned()),
            (
                "other scripts' format marks",
                "\u{0600}\u{06dd}\u{070f}\u{110bd}\u{1d173}".to_owned(),
            ),
            ("line separators", "one\u{2028}two\u{2029}".to_owned()),
            ("ESC sequence", "\u{1b}[2J\u{1b}]0;pwn\u{7}".to_owned()),
            ("C0 controls", "a\u{0}b\u{1}c\u{8}d\r\u{b}\u{c}".to_owned()),
            ("DEL and C1", "a\u{7f}b\u{80}c\u{9b}31m\u{9f}".to_owned()),
        ]
    }

    #[test]
    fn a_body_never_carries_a_hidden_character_whichever_way_it_is_built() {
        for (class, hostile) in hostile_table() {
            let built = Body::new(hostile.clone()).unwrap();
            let json = serde_json::to_string(&hostile).unwrap();
            let read: Body = serde_json::from_str(&json).unwrap();
            for body in [&built, &read] {
                assert!(
                    !body.as_str().chars().any(is_hidden),
                    "{class}: {:?}",
                    body.as_str()
                );
                assert_eq!(
                    body.as_str().chars().count(),
                    hostile.chars().count(),
                    "{class}: one replacement per character"
                );
                assert!(body.as_str().contains(REPLACEMENT), "{class}");
            }
            assert_eq!(built, read, "{class}");
        }
    }

    #[test]
    fn the_tag_block_is_hidden_end_to_end_assigned_or_not() {
        for code in 0xe0000..=0xe007f_u32 {
            let c = char::from_u32(code).unwrap();
            assert!(is_hidden(c), "U+{code:X}");
        }
        assert!(!is_hidden('\u{e0080}'));
        assert!(!is_hidden('\u{dffff}'));
    }

    #[test]
    fn a_replaced_body_names_where_it_was_touched_and_nothing_else() {
        assert_eq!(
            Body::new("pay\u{200b}pal").unwrap().as_str(),
            "pay\u{fffd}pal"
        );
        let tagged = Body::new("ok\u{e0069}\u{e0067}").unwrap();
        assert_eq!(tagged.as_str(), "ok\u{fffd}\u{fffd}");
        // Built from what it serialised to, it is unchanged: neutralising is
        // idempotent, so a message relayed hop to hop does not drift.
        let again = Body::new(tagged.to_string()).unwrap();
        assert_eq!(again, tagged);
    }

    #[test]
    fn prose_a_person_can_see_is_carried_exactly() {
        for prose in [
            "ready to hand this off",
            "line one\n\tindented line two\n",
            "café, naïve, Straße",
            "שלום עולם and مرحبا",
            "日本語のテキスト",
            "shipping it 🚀 👍",
            "quotes \" and backslashes \\ survive",
        ] {
            assert_eq!(Body::new(prose).unwrap().as_str(), prose);
        }
    }

    #[test]
    fn a_body_of_nothing_but_hidden_characters_is_still_one_a_reader_can_see() {
        let body = Body::new("\u{200b}\u{e0041}\u{1b}").unwrap();
        assert_eq!(body.as_str(), "\u{fffd}\u{fffd}\u{fffd}");
    }

    /// The router checks a stored body's bytes against four per character
    /// before it rebuilds it here, so a replacement must never grow a body
    /// past that — the widest thing it replaces is shorter than four bytes or
    /// equal to it, and [`REPLACEMENT`] is three.
    #[test]
    fn a_body_at_the_cap_stays_within_four_bytes_a_character_after_replacement() {
        assert_eq!(REPLACEMENT.len_utf8(), 3);
        for hostile in ['\u{1}', '\u{80}', '\u{200b}', '\u{e0041}'] {
            let at_cap: String = std::iter::repeat_n(hostile, MAX_BODY_CHARS).collect();
            let body = Body::new(at_cap).unwrap();
            assert_eq!(body.as_str().chars().count(), MAX_BODY_CHARS);
            assert!(body.as_str().len() <= 4 * MAX_BODY_CHARS);
        }
    }

    /// A message stored before this rule existed comes back from the router's
    /// store as a plain string and is rebuilt with `Body::new`. It is carried
    /// neutralised rather than refused, so it reaches no reader unfiltered and
    /// cannot fail the page it sits on.
    #[test]
    fn a_body_stored_before_the_rule_is_read_back_neutralised_not_refused() {
        let stored = "legacy \u{202e}txt\u{e0041}\u{e0042} note".to_owned();
        let body = Body::new(stored).unwrap();
        assert_eq!(body.as_str(), "legacy \u{fffd}txt\u{fffd}\u{fffd} note");
    }

    fn full_room(scope: Scope) -> RoomId {
        RoomId::new(ProjectId::try_from("example-repo").unwrap(), scope)
    }

    #[test]
    fn room_id_round_trips_every_scope() {
        for room in [
            full_room(Scope::Ticket(TicketId::new("ENG", 557).unwrap())),
            full_room(Scope::Repo(
                RepoSlug::try_from("example-org/example-repo").unwrap(),
            )),
            full_room(Scope::Project),
        ] {
            let json = serde_json::to_string(&room).unwrap();
            assert_eq!(serde_json::from_str::<RoomId>(&json).unwrap(), room);
        }
    }

    #[test]
    fn scope_is_closed_and_refuses_a_kind_it_does_not_know() {
        for hostile in [
            r#""urgent""#,
            r#"{"ticket":"ENG-557","extra":true}"#,
            r#"{"repo":"example-org/example-repo","ticket":"ENG-557"}"#,
        ] {
            assert!(
                serde_json::from_str::<Scope>(hostile).is_err(),
                "accepted {hostile}"
            );
        }
    }

    /// Pins the actual enforcer for a two-tag `Scope` object. Nothing about
    /// `Scope`'s own `deny_unknown_fields` does this — it has no effect on an
    /// externally tagged enum whose variants are newtype/unit, since none of
    /// them has named fields to check. It is serde_json's single-key-map rule
    /// for externally tagged enums that refuses the second key before either
    /// variant is even chosen, and this test exists so that fact cannot
    /// regress silently if serde's tagging behaviour ever changes.
    #[test]
    fn scope_refuses_a_two_key_object() {
        let two_keys = r#"{"ticket":"ENG-557","repo":"example-org/example-repo"}"#;
        assert!(serde_json::from_str::<Scope>(two_keys).is_err());
    }

    /// A unit variant's externally tagged JSON form is a bare string, but
    /// serde also accepts the single-key-object spelling with a `null`
    /// payload, since a unit variant's "content" deserializes from `null`.
    /// Documented rather than hidden: a peer that sends `{"project":null}`
    /// instead of `"project"` is not sending anything `Scope` refuses.
    #[test]
    fn scope_project_accepts_both_its_bare_and_object_spelling() {
        assert_eq!(
            serde_json::from_str::<Scope>(r#""project""#).unwrap(),
            Scope::Project
        );
        assert_eq!(
            serde_json::from_str::<Scope>(r#"{"project":null}"#).unwrap(),
            Scope::Project
        );
    }

    #[test]
    fn room_id_refuses_an_unknown_field() {
        let mut value = serde_json::to_value(full_room(Scope::Project)).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("note".to_owned(), serde_json::Value::String("hi".into()));
        assert!(serde_json::from_value::<RoomId>(value).is_err());
    }

    /// Everything a job's free-text fields could carry is still refused by
    /// every type here, the same way `sealmesh-wire`'s canary test checks
    /// `Event`. `Body` is the one exception by design — a chat message is
    /// prose — so this only exercises the closed, capped types around it.
    #[test]
    fn nothing_a_job_file_carries_can_be_assigned_to_a_closed_chat_type() {
        let prose = [
            "pasting the thread from #legal: Dart says the Acme settlement number is 4.2M",
            "/Users/sandbank/clients/acme-litigation",
            "https://github.com/example-org/example-repo/pull/627",
        ];
        for text in prose {
            assert!(
                serde_json::from_str::<MessageKind>(&format!("{text:?}")).is_err(),
                "kind accepted {text:?}"
            );
            assert!(
                TicketId::try_from(text).is_err(),
                "ticket accepted {text:?}"
            );
            assert!(RepoSlug::try_from(text).is_err(), "repo accepted {text:?}");
            assert!(
                ProjectId::try_from(text).is_err(),
                "project accepted {text:?}"
            );
        }
    }

    /// `sealmesh-wire` is the format both a snapshot producer and consumer
    /// depend on. If it depended back on this crate, "does `sealmesh-wire`
    /// have a `String` field" would stop being answerable by reading one
    /// crate — see the module doc. This reads the workspace's resolved
    /// lockfile rather than trusting that nobody adds the edge later.
    #[test]
    fn sealmesh_wire_does_not_depend_on_chat_wire() {
        #[derive(Deserialize)]
        struct Lockfile {
            package: Vec<Package>,
        }

        #[derive(Deserialize)]
        struct Package {
            name: String,
            #[serde(default)]
            dependencies: Vec<String>,
        }

        let lock_path = format!("{}/../../Cargo.lock", env!("CARGO_MANIFEST_DIR"));
        let lock_text = std::fs::read_to_string(&lock_path)
            .unwrap_or_else(|e| panic!("could not read {lock_path}: {e}"));
        let lockfile: Lockfile = toml::from_str(&lock_text).unwrap();

        let wire = lockfile
            .package
            .iter()
            .find(|p| p.name == "sealmesh-wire")
            .expect("sealmesh-wire is not a workspace member");

        assert!(
            !wire
                .dependencies
                .iter()
                .any(|d| d.starts_with("sealmesh-chat-wire")),
            "sealmesh-wire depends on sealmesh-chat-wire: {:?}",
            wire.dependencies
        );
    }
}
