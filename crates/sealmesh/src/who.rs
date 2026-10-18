use std::{collections::BTreeSet, ops::ControlFlow};

use anyhow::{Context, Result};
use jiff::Timestamp;
use sealmesh_proto::{ClientFrame, HostState, ProjectEntry, QueryId, Role, ServerFrame};

use crate::{
    client::{self, Connected},
    config::Config,
    enrich::github,
    progress::{Phase, Progress},
    render::{self, HostStatus, PrStatuses},
    wire::{RepoSlug, Snapshot},
};

pub enum HostReport {
    Answering(Box<Snapshot>),
    NotSharing { host: String },
    Unreachable { host: String },
    Throttled { host: String },
    Unreadable { host: String, reason: String },
}

/// What one query came back with. `undecodable` counts answers this build could
/// not read at all — the host they belonged to is unknowable, because the
/// failure happens before the frame's `host` field can be recovered, so they
/// cannot be reported as rows. The count is the only trace they leave, and
/// dropping it silently would make a partial answer look like a complete one.
///
/// `projects` is empty both when the router has no mapping configured and
/// when it never answered `ClientFrame::Projects` at all (an old router) —
/// the two are different situations (see `client::request_projects`), but
/// they render identically: everything ungrouped.
///
/// `partial` is the router saying it withheld work this login cannot read.
/// It is one bit for the whole query — the router sends nothing about which
/// machine, which repo or how much — and it is what keeps a narrowed answer
/// from reading as a quiet fleet.
pub struct Answers {
    pub reports: Vec<HostReport>,
    pub undecodable: usize,
    pub projects: Vec<ProjectEntry>,
    pub partial: bool,
}

pub async fn fetch_all(config: &Config, progress: &Progress) -> Result<Answers> {
    progress.set(Phase::Connecting);
    let Connected { socket, host } = client::connect(&config.server, Role::Reader).await?;
    let _ = host;
    ask(socket, |phase| {
        progress.set(phase);
        ControlFlow::Continue(())
    })
    .await
}

/// Asks one question on a reader connection that has already been welcomed,
/// and folds the answer. This is the whole reader path after the handshake —
/// the project mapping, the `Ask`, and the rule-21 attribution check in
/// [`interpret`] — so every reader of the fleet goes through it rather than
/// through a copy.
///
/// `on_phase` hears each change in progress. Returning `Break` abandons the
/// query with an error: a caller that has to bound how many machines it will
/// hold in memory stops here, before the next report is kept, because the
/// router sending the reports is a service that can itself be compromised.
pub async fn ask(
    mut socket: client::Socket,
    mut on_phase: impl FnMut(Phase) -> ControlFlow<()>,
) -> Result<Answers> {
    let projects = client::request_projects(&mut socket)
        .await?
        .unwrap_or_default();

    let id = QueryId(1);
    if on_phase(Phase::Asking { answered: 0 }).is_break() {
        anyhow::bail!("the query was abandoned before it was asked");
    }
    client::send(&mut socket, &ClientFrame::Ask { id }).await?;

    let mut collected = Collected::default();
    loop {
        let incoming = client::recv(&mut socket).await?;
        let answered = collected.reports.len();
        match fold(id, &mut collected, incoming) {
            Folded::Reading => {
                // One `Report` is one machine that has answered. The count is
                // the honest quantity to show: an undecodable frame is not
                // counted, because it is not necessarily a report at all.
                if collected.reports.len() != answered
                    && on_phase(Phase::Asking {
                        answered: collected.reports.len(),
                    })
                    .is_break()
                {
                    anyhow::bail!("the query was abandoned after too many answers");
                }
            }
            Folded::Complete => {
                let Collected {
                    reports,
                    undecodable,
                    partial,
                } = collected;
                return Ok(Answers {
                    reports,
                    undecodable,
                    projects,
                    partial,
                });
            }
            Folded::Denied => anyhow::bail!(
                "the router refused this login. Check `gh auth status`, and that you are \
                 an active member of the GitHub organisation this router serves"
            ),
        }
    }
}

/// What one query has gathered so far.
#[derive(Default)]
struct Collected {
    reports: Vec<HostReport>,
    undecodable: usize,
    partial: bool,
}

/// Whether to keep reading after one incoming frame.
enum Folded {
    Reading,
    Complete,
    Denied,
}

/// Folds one incoming frame into what query `id` has gathered.
///
/// Its own function so the one thing that must not be lost — the router saying
/// it withheld work — is checked by a test rather than by reading a socket
/// loop. A reader that dropped `Partial` would show narrowed rows as the whole
/// answer, which is the failure the notice exists to prevent.
fn fold(id: QueryId, collected: &mut Collected, incoming: client::Incoming) -> Folded {
    let frame = match incoming {
        client::Incoming::Frame(frame) => frame,
        client::Incoming::Noise => return Folded::Reading,
        // A reader never holds a room, so a room refusal is as foreign to it
        // as any frame it cannot read.
        client::Incoming::Undecodable | client::Incoming::UnknownRefusal(_) => {
            collected.undecodable = collected.undecodable.saturating_add(1);
            return Folded::Reading;
        }
    };
    match frame {
        ServerFrame::Report {
            id: reported,
            host,
            state,
        } if reported == id => {
            collected.reports.push(interpret(host, state));
            Folded::Reading
        }
        // The router withheld work this login cannot read. Held until the
        // rows are rendered, because the notice goes after them.
        ServerFrame::Partial { id: reported } if reported == id => {
            collected.partial = true;
            Folded::Reading
        }
        ServerFrame::Complete { id: reported } if reported == id => Folded::Complete,
        ServerFrame::Denied {} => Folded::Denied,
        _ => Folded::Reading,
    }
}

fn interpret(attributed: sealmesh_wire::HostId, state: HostState) -> HostReport {
    let host = attributed.as_str().to_owned();
    match state {
        HostState::Unreachable {} => HostReport::Unreachable { host },
        HostState::NotSharing {} => HostReport::NotSharing { host },
        HostState::Throttled {} => HostReport::Throttled { host },
        HostState::Answering { snapshot } if snapshot.host != attributed => {
            HostReport::Unreadable {
                host,
                reason: "answer claims to be a different host".to_owned(),
            }
        }
        HostState::Answering { snapshot } => HostReport::Answering(snapshot),
    }
}

pub fn referenced_prs(reports: &[HostReport]) -> Vec<(RepoSlug, u32)> {
    let mut seen: BTreeSet<(RepoSlug, u32)> = BTreeSet::new();
    for entry in reports {
        let HostReport::Answering(snapshot) = entry else {
            continue;
        };
        for agent in &snapshot.agents {
            if let Some(pr) = agent.pr {
                seen.insert((agent.repo.clone(), pr));
            }
        }
    }
    seen.into_iter().collect()
}

pub fn present(
    reports: &[HostReport],
    projects: &[ProjectEntry],
    prs: &PrStatuses,
    now: Timestamp,
    color: bool,
    partial: bool,
) -> String {
    let statuses: Vec<HostStatus<'_>> = reports
        .iter()
        .map(|entry| match entry {
            HostReport::Answering(snapshot) => HostStatus::Answering(snapshot),
            HostReport::NotSharing { host } => HostStatus::NotSharing { host: host.clone() },
            HostReport::Unreachable { host } => HostStatus::Unreachable { host: host.clone() },
            HostReport::Throttled { host } => HostStatus::Throttled { host: host.clone() },
            HostReport::Unreadable { host, reason } => HostStatus::Unreadable {
                host: host.clone(),
                reason: reason.clone(),
            },
        })
        .collect();
    render::render_answer(&statuses, projects, prs, now, color, partial)
}

/// Everything the query produced, held until the progress line is gone.
struct Gathered {
    reports: Vec<HostReport>,
    undecodable: usize,
    projects: Vec<ProjectEntry>,
    partial: bool,
    prs: PrStatuses,
    now: Timestamp,
}

pub async fn run(config: &Config) -> Result<()> {
    let progress = Progress::start();
    // `gather` is a separate function so that `finish` runs on the error paths
    // too. A `?` here would skip it and leave an error message printed
    // underneath a live progress line.
    let gathered = gather(config, &progress).await;
    progress.finish().await;

    let Gathered {
        reports,
        undecodable,
        projects,
        partial,
        prs,
        now,
    } = gathered?;

    let color = std::io::IsTerminal::is_terminal(&std::io::stdout()) && render::styling_allowed();
    print!(
        "{}",
        present(&reports, &projects, &prs, now, color, partial)
    );
    if undecodable > 0 {
        eprintln!(
            "note: {undecodable} answer(s) could not be read by this build and are missing \
             from the list above. Upgrade sealmesh."
        );
    }
    Ok(())
}

async fn gather(config: &Config, progress: &Progress) -> Result<Gathered> {
    let Answers {
        reports,
        undecodable,
        projects,
        partial,
    } = fetch_all(config, progress)
        .await
        .context("asking the router")?;
    let now = Timestamp::now();
    let prs = github::resolve(&referenced_prs(&reports), now, progress).await;
    Ok(Gathered {
        reports,
        undecodable,
        projects,
        partial,
        prs,
        now,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use sealmesh_wire::HostId;

    fn snapshot(host: &str) -> Box<Snapshot> {
        Box::new(
            Snapshot::from_json(
                format!(
                    r#"{{"schema":2,"host":"{host}",
                         "collected_at":"2026-08-10T19:56:51.322Z","agents":[]}}"#
                )
                .as_bytes(),
            )
            .unwrap(),
        )
    }

    fn host(name: &str) -> HostId {
        HostId::try_from(name).unwrap()
    }

    #[test]
    fn an_answer_cannot_claim_a_host_the_router_did_not_attribute_it_to() {
        let report = interpret(
            host("shelter"),
            HostState::Answering {
                snapshot: snapshot("sandbank"),
            },
        );
        let out = present(
            &[report],
            &[],
            &PrStatuses::new(),
            "2026-08-10T19:57:01.322Z".parse().unwrap(),
            false,
            false,
        );
        assert!(out.contains("shelter"), "{out}");
        assert!(!out.contains("sandbank"), "{out}");
        assert!(out.contains("unreadable"), "{out}");
    }

    #[test]
    fn a_matching_attribution_renders_the_snapshot() {
        let report = interpret(
            host("shelter"),
            HostState::Answering {
                snapshot: snapshot("shelter"),
            },
        );
        assert!(matches!(report, HostReport::Answering(_)));
    }

    #[test]
    fn one_unreadable_answer_does_not_hide_the_other_machine() {
        let reports = vec![
            HostReport::Answering(snapshot("sandbank")),
            HostReport::Unreadable {
                host: "shelter".to_owned(),
                reason: "answer is not valid json for this schema (line 1, column 4)".to_owned(),
            },
        ];
        let out = present(
            &reports,
            &[],
            &PrStatuses::new(),
            "2026-08-10T19:57:01.322Z".parse().unwrap(),
            false,
            false,
        );
        assert!(out.contains("sandbank"), "{out}");
        assert!(out.contains("shelter — unreadable"), "{out}");
    }

    fn frame(frame: ServerFrame) -> client::Incoming {
        client::Incoming::Frame(frame)
    }

    #[test]
    fn a_partial_frame_sets_the_flag_the_notice_is_rendered_from() {
        let id = QueryId(1);
        let mut collected = Collected::default();
        for incoming in [
            frame(ServerFrame::Report {
                id,
                host: host("sandbank"),
                state: HostState::Answering {
                    snapshot: snapshot("sandbank"),
                },
            }),
            frame(ServerFrame::Partial { id }),
        ] {
            assert!(matches!(
                fold(id, &mut collected, incoming),
                Folded::Reading
            ));
        }
        assert!(matches!(
            fold(id, &mut collected, frame(ServerFrame::Complete { id })),
            Folded::Complete
        ));
        assert!(collected.partial, "a Partial frame was dropped");

        let out = present(
            &collected.reports,
            &[],
            &PrStatuses::new(),
            "2026-08-10T19:57:01.322Z".parse().unwrap(),
            false,
            collected.partial,
        );
        assert!(out.contains("sandbank"), "{out}");
        assert!(out.contains("this answer is partial"), "{out}");
    }

    #[test]
    fn an_answer_without_a_partial_frame_is_not_marked_partial() {
        let id = QueryId(1);
        let mut collected = Collected::default();
        fold(
            id,
            &mut collected,
            frame(ServerFrame::Report {
                id,
                host: host("sandbank"),
                state: HostState::NotSharing {},
            }),
        );
        assert!(matches!(
            fold(id, &mut collected, frame(ServerFrame::Complete { id })),
            Folded::Complete
        ));
        assert!(!collected.partial);
        assert_eq!(collected.reports.len(), 1);
    }

    #[test]
    fn a_partial_frame_for_another_query_does_not_mark_this_one() {
        let mut collected = Collected::default();
        fold(
            QueryId(1),
            &mut collected,
            frame(ServerFrame::Partial { id: QueryId(2) }),
        );
        assert!(!collected.partial);
    }

    #[test]
    fn noise_and_unreadable_frames_keep_reading_and_denied_stops() {
        let id = QueryId(1);
        let mut collected = Collected::default();
        assert!(matches!(
            fold(id, &mut collected, client::Incoming::Noise),
            Folded::Reading
        ));
        assert!(matches!(
            fold(id, &mut collected, client::Incoming::Undecodable),
            Folded::Reading
        ));
        assert_eq!(collected.undecodable, 1);
        assert!(matches!(
            fold(id, &mut collected, frame(ServerFrame::Denied {})),
            Folded::Denied
        ));
    }

    #[test]
    fn a_partial_answer_is_presented_with_its_notice_and_a_complete_one_without() {
        let reports = vec![interpret(
            host("sandbank"),
            HostState::Answering {
                snapshot: snapshot("sandbank"),
            },
        )];
        let now = "2026-08-10T19:57:01.322Z".parse().unwrap();

        let partial = present(&reports, &[], &PrStatuses::new(), now, false, true);
        assert!(partial.contains("sandbank"), "{partial}");
        assert!(partial.contains("this answer is partial"), "{partial}");

        let complete = present(&reports, &[], &PrStatuses::new(), now, false, false);
        assert!(!complete.contains("partial"), "{complete}");
    }

    #[test]
    fn the_transport_states_map_to_distinct_rows() {
        let reports = vec![
            interpret(host("a"), HostState::Unreachable {}),
            interpret(host("b"), HostState::NotSharing {}),
        ];
        let out = present(
            &reports,
            &[],
            &PrStatuses::new(),
            "2026-08-10T19:57:01.322Z".parse().unwrap(),
            false,
            false,
        );
        assert!(out.contains("a — unreachable"), "{out}");
        assert!(out.contains("b — not sharing"), "{out}");
    }

    #[test]
    fn a_throttled_host_is_its_own_row_never_unreachable_answering_or_not_sharing() {
        let report = interpret(host("shelter"), HostState::Throttled {});
        assert!(matches!(&report, HostReport::Throttled { host } if host == "shelter"));

        let out = present(
            &[report],
            &[],
            &PrStatuses::new(),
            "2026-08-10T19:57:01.322Z".parse().unwrap(),
            false,
            false,
        );
        assert!(out.contains("shelter — throttled"), "{out}");
        for other in ["unreachable", "not sharing", "agent", "unreadable"] {
            assert!(!out.contains(other), "`{other}` in {out}");
        }
    }

    /// `HostState` as a reader built before `Throttled` knows it.
    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize)]
    #[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
    enum HostStateBeforeThrottled {
        Answering { snapshot: Box<Snapshot> },
        NotSharing,
        Unreachable,
    }

    /// `ServerFrame` as a reader built before `Throttled` knows it.
    #[allow(dead_code)]
    #[derive(Debug, serde::Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    enum ServerFrameBeforeThrottled {
        Welcome {
            host: HostId,
        },
        Query {
            id: QueryId,
            asked_by: HostId,
        },
        Report {
            id: QueryId,
            host: HostId,
            state: HostStateBeforeThrottled,
        },
        Complete {
            id: QueryId,
        },
        Projects {
            projects: Vec<ProjectEntry>,
        },
        Partial {
            id: QueryId,
        },
        Denied,
    }

    /// `client::classify` as a reader built before `Throttled` runs it: a
    /// frame its own enum cannot decode is `Undecodable`, and one it can is a
    /// frame. (Every frame that build decodes, this build decodes the same.)
    fn classify_before_throttled(bytes: &[u8]) -> client::Incoming {
        match sealmesh_proto::decode::<ServerFrameBeforeThrottled>(bytes) {
            Ok(_) => client::Incoming::Frame(sealmesh_proto::decode(bytes).unwrap()),
            Err(_) => client::Incoming::Undecodable,
        }
    }

    /// What a new router sends for one query: one machine answering, one
    /// throttled, one gone.
    fn mixed_answer(id: QueryId) -> Vec<Vec<u8>> {
        [
            ServerFrame::Report {
                id,
                host: host("sandbank"),
                state: HostState::Answering {
                    snapshot: snapshot("sandbank"),
                },
            },
            ServerFrame::Report {
                id,
                host: host("shelter"),
                state: HostState::Throttled {},
            },
            ServerFrame::Report {
                id,
                host: host("prism"),
                state: HostState::Unreachable {},
            },
            ServerFrame::Complete { id },
        ]
        .iter()
        .map(|frame| serde_json::to_vec(frame).unwrap())
        .collect()
    }

    fn fold_all(id: QueryId, frames: Vec<client::Incoming>) -> Collected {
        let mut collected = Collected::default();
        let mut completed = false;
        for incoming in frames {
            match fold(id, &mut collected, incoming) {
                Folded::Reading => assert!(!completed, "a frame arrived after Complete"),
                Folded::Complete => completed = true,
                Folded::Denied => panic!("a throttled report ended the query"),
            }
        }
        assert!(completed, "the query never completed");
        collected
    }

    #[test]
    fn an_old_reader_skips_a_throttled_report_and_keeps_every_other_row() {
        let id = QueryId(1);
        let frames = mixed_answer(id)
            .iter()
            .map(|bytes| classify_before_throttled(bytes))
            .collect();
        let collected = fold_all(id, frames);

        assert_eq!(collected.undecodable, 1);
        let out = present(
            &collected.reports,
            &[],
            &PrStatuses::new(),
            "2026-08-10T19:57:01.322Z".parse().unwrap(),
            false,
            false,
        );
        assert!(out.contains("sandbank — 0 agents"), "{out}");
        assert!(out.contains("prism — unreachable"), "{out}");
        assert!(!out.contains("shelter"), "{out}");
    }

    #[test]
    fn an_upgraded_reader_renders_the_same_answer_with_the_throttled_row() {
        let id = QueryId(1);
        let frames = mixed_answer(id)
            .iter()
            .map(|bytes| client::Incoming::Frame(sealmesh_proto::decode(bytes).unwrap()))
            .collect();
        let collected = fold_all(id, frames);

        assert_eq!(collected.undecodable, 0);
        let out = present(
            &collected.reports,
            &[],
            &PrStatuses::new(),
            "2026-08-10T19:57:01.322Z".parse().unwrap(),
            false,
            false,
        );
        assert!(out.contains("sandbank — 0 agents"), "{out}");
        assert!(out.contains("prism — unreachable"), "{out}");
        assert!(out.contains("shelter — throttled"), "{out}");
        assert!(!out.contains("shelter — unreachable"), "{out}");
    }
}
