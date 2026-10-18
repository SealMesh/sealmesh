use std::{
    fs::OpenOptions,
    io::Write as _,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use jiff::Timestamp;
use sealmesh_chat_wire::RoomId;
use sealmesh_wire::HostId;
use serde::{Deserialize, Serialize};

use crate::room::local::{BackfillCategory, PostCategory};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub at: Timestamp,
    pub asked_by: HostId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents: Option<usize>,
    #[serde(default)]
    pub refused: bool,
}

/// A room event in the ledger (rules 16, 28). Never a body: a refusal says
/// which room and which category, and received traffic is a count per room,
/// per sender host, per interval.
///
/// Tagged by `room_event`, which a query `Entry` never carries, and a query
/// entry's `asked_by` is required, so neither shape reads as the other and a
/// build that knows only `Entry` skips these lines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "room_event", rename_all = "snake_case", deny_unknown_fields)]
pub enum RoomRecord {
    /// The router refused one of this machine's posts.
    PostRefused {
        at: Timestamp,
        room: RoomId,
        category: PostCategory,
    },
    /// The router refused a history read with a category this build does not
    /// know. Known categories are routine and not recorded.
    BackfillRefused {
        at: Timestamp,
        room: RoomId,
        category: BackfillCategory,
    },
    /// Messages received in `room` from `from` between `since` and `at`.
    Received {
        at: Timestamp,
        since: Timestamp,
        room: RoomId,
        from: HostId,
        messages: u64,
    },
    /// Messages received between `since` and `at` from more room and sender
    /// pairs than one interval records by name.
    ReceivedElsewhere {
        at: Timestamp,
        since: Timestamp,
        messages: u64,
    },
    /// Refusals between `since` and `at` beyond the most one interval records
    /// one by one.
    RefusalsNotItemised {
        at: Timestamp,
        since: Timestamp,
        refusals: u64,
    },
}

/// One line of the ledger, whichever kind it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Query(Entry),
    Room(RoomRecord),
}

pub fn path() -> Option<PathBuf> {
    crate::paths::state_home().map(|d| d.join("sealmesh/audit.jsonl"))
}

pub fn record(path: &Path, entry: &Entry) -> Result<()> {
    append(path, entry)
}

pub fn record_room(path: &Path, entry: &RoomRecord) -> Result<()> {
    append(path, entry)
}

fn append<T: Serialize>(path: &Path, entry: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let mut line = serde_json::to_string(entry).context("encoding an audit entry")?;
    line.push('\n');

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    file.write_all(line.as_bytes())
        .with_context(|| format!("appending to {}", path.display()))
}

pub fn read(path: &Path) -> Result<Vec<Entry>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    Ok(text
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect())
}

/// Every line of the ledger this build can read, in order.
pub fn read_all(path: &Path) -> Result<Vec<Record>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    Ok(text
        .lines()
        .filter_map(|line| {
            serde_json::from_str(line)
                .map(Record::Room)
                .or_else(|_| serde_json::from_str(line).map(Record::Query))
                .ok()
        })
        .collect())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn entry(asked_by: &str, agents: Option<usize>) -> Entry {
        Entry {
            at: "2026-08-13T12:00:00Z".parse().unwrap(),
            asked_by: HostId::try_from(asked_by).unwrap(),
            agents,
            refused: false,
        }
    }

    #[test]
    fn an_entry_written_before_the_refusal_field_existed_still_reads_back() {
        let old = r#"{"at":"2026-08-13T12:00:00Z","asked_by":"shelter","agents":2}"#;
        let back: Entry = serde_json::from_str(old).unwrap();
        assert_eq!(back.agents, Some(2));
        assert!(!back.refused, "an old entry must not read as a refusal");
    }

    fn temp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sealmesh-audit-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join("audit.jsonl")
    }

    #[test]
    fn entries_append_and_read_back_in_order() {
        let path = temp("append");
        record(&path, &entry("shelter", Some(2))).unwrap();
        record(&path, &entry("sandbank", None)).unwrap();

        let back = read(&path).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back.first().unwrap().asked_by.as_str(), "shelter");
        assert_eq!(back.last().unwrap().agents, None);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn a_missing_log_reads_as_no_history_rather_than_an_error() {
        assert_eq!(read(Path::new("/nonexistent/audit.jsonl")).unwrap(), vec![]);
    }

    #[test]
    fn a_truncated_final_line_does_not_hide_the_entries_before_it() {
        let path = temp("truncated");
        record(&path, &entry("shelter", Some(2))).unwrap();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{\"at\":\"2026-08").unwrap();
        drop(file);

        let back = read(&path).unwrap();
        assert_eq!(back.len(), 1);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    fn room() -> RoomId {
        RoomId::new(
            sealmesh_wire::ProjectId::try_from("example-repo").unwrap(),
            sealmesh_chat_wire::Scope::Project,
        )
    }

    #[test]
    fn room_records_and_query_entries_share_the_ledger_without_reading_as_each_other() {
        let path = temp("rooms");
        let at: Timestamp = "2026-09-18T12:00:00Z".parse().unwrap();
        record(&path, &entry("shelter", Some(1))).unwrap();
        record_room(
            &path,
            &RoomRecord::PostRefused {
                at,
                room: room(),
                category: crate::room::local::Category::UNKNOWN,
            },
        )
        .unwrap();
        record_room(
            &path,
            &RoomRecord::Received {
                at,
                since: at,
                room: room(),
                from: HostId::try_from("shelter").unwrap(),
                messages: 3,
            },
        )
        .unwrap();

        // A build that knows only query entries still reads its own.
        assert_eq!(read(&path).unwrap().len(), 1);
        let all = read_all(&path).unwrap();
        assert_eq!(all.len(), 3);
        assert!(matches!(all.first(), Some(Record::Query(_))));
        assert!(matches!(
            all.get(1),
            Some(Record::Room(RoomRecord::PostRefused { .. }))
        ));
        assert!(matches!(
            all.get(2),
            Some(Record::Room(RoomRecord::Received { messages: 3, .. }))
        ));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn a_room_record_has_nowhere_for_a_body() {
        let line = r#"{"room_event":"received","at":"2026-09-18T12:00:00Z","since":"2026-09-18T12:00:00Z","room":{"project":"example-repo","scope":"project"},"from":"shelter","messages":1,"body":"hi"}"#;
        assert!(serde_json::from_str::<RoomRecord>(line).is_err());
        let refused = r#"{"room_event":"post_refused","at":"2026-09-18T12:00:00Z","room":{"project":"example-repo","scope":"project"},"category":"rate_limited","body":"hi"}"#;
        assert!(serde_json::from_str::<RoomRecord>(refused).is_err());
    }

    #[test]
    fn an_entry_carries_no_agent_detail() {
        let json = serde_json::to_string(&entry("shelter", Some(2))).unwrap();
        for leaked in ["branch", "repo", "ticket", "run_state", "feature/"] {
            assert!(!json.contains(leaked), "{json}");
        }
    }
}
