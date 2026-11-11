use std::{collections::BTreeSet, ops::ControlFlow};

use anyhow::{Context, Result};
use jiff::Timestamp;
use sealmesh_proto::{
    ClientFrame, HostState, MachineName, ProjectEntry, QueryId, Role, ServerFrame,
};

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

/// One row of the answer: a host's report, and which of its machines it came
/// from. `machine` is `None` from a router or daemon built before machine
/// names, and for a host with nothing connected.
pub struct Row {
    pub machine: Option<MachineName>,
    pub report: HostReport,
}

impl From<HostReport> for Row {
    fn from(report: HostReport) -> Self {
        Self {
            machine: None,
            report,
        }
    }
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
    pub reports: Vec<Row>,
    pub undecodable: usize,
    pub projects: Vec<ProjectEntry>,
    pub partial: bool,
}

pub async fn fetch_all(config: &Config, progress: &Progress) -> Result<Answers> {
    progress.set(Phase::Connecting);
    let Connected { socket, host } = client::connect(config, Role::Reader).await?;
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
///
/// # Asking by machine, on any router
///
/// It asks with `AskMachines`, so each row says which of a host's machines it
/// came from, and sends the project map request right behind it. A router
/// processes one reader frame at a time, which makes the second request a
/// fence: a router that knows `AskMachines` answers it in full before it gets
/// to `Projects`, and a router built before it skips it and answers
/// `Projects` first. So a `Projects` reply arriving before any answer means
/// the question went unheard, and it is asked again with `Ask` — with no
/// timeout to wait out and no fan-out run twice.
pub async fn ask(
    mut socket: client::Socket,
    mut on_phase: impl FnMut(Phase) -> ControlFlow<()>,
) -> Result<Answers> {
    let id = QueryId(1);
    if on_phase(Phase::Asking { answered: 0 }).is_break() {
        anyhow::bail!("the query was abandoned before it was asked");
    }
    client::send(&mut socket, &ClientFrame::AskMachines { id }).await?;
    client::send(&mut socket, &ClientFrame::Projects {}).await?;

    let mut collected = Collected::default();
    let mut heard = false;
    let mut projects: Option<Vec<ProjectEntry>> = None;
    loop {
        let incoming = client::recv(&mut socket).await?;
        if let client::Incoming::Frame(ServerFrame::Projects { projects: map }) = incoming {
            if !heard {
                // The fence came back first: this router skipped
                // `AskMachines`. Ask the way it understands.
                heard = true;
                client::send(&mut socket, &ClientFrame::Ask { id }).await?;
            }
            projects.get_or_insert(map);
            continue;
        }
        heard |= answers(id, &incoming);
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
                // A router that answered `AskMachines` gets to `Projects`
                // only now. Bounded, like any wait for the map.
                let projects = match projects {
                    Some(projects) => projects,
                    None => client::await_projects(&mut socket)
                        .await?
                        .unwrap_or_default(),
                };
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

/// Whether `incoming` is part of the router's answer to query `id`, which
/// tells the reader the router heard the question.
fn answers(id: QueryId, incoming: &client::Incoming) -> bool {
    match incoming {
        client::Incoming::Frame(
            ServerFrame::Report { id: reported, .. }
            | ServerFrame::MachineReport { id: reported, .. }
            | ServerFrame::Partial { id: reported }
            | ServerFrame::Complete { id: reported },
        ) => *reported == id,
        _ => false,
    }
}

/// What one query has gathered so far.
#[derive(Default)]
struct Collected {
    reports: Vec<Row>,
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
            collected.reports.push(interpret(host, state).into());
            Folded::Reading
        }
        // The same answer with its machine: what a router sends for
        // `AskMachines`. The attribution check is the same one.
        ServerFrame::MachineReport {
            id: reported,
            host,
            machine,
            state,
        } if reported == id => {
            collected.reports.push(Row {
                machine,
                report: interpret(host, state),
            });
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

pub fn referenced_prs(reports: &[Row]) -> Vec<(RepoSlug, u32)> {
    let mut seen: BTreeSet<(RepoSlug, u32)> = BTreeSet::new();
    for entry in reports {
        let HostReport::Answering(snapshot) = &entry.report else {
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

/// The rows as `who` prints them. A row with a machine is labelled
/// `host@machine`, so one person's machines read as theirs and apart.
pub fn present(
    reports: &[Row],
    projects: &[ProjectEntry],
    prs: &PrStatuses,
    now: Timestamp,
    color: bool,
    partial: bool,
) -> String {
    let statuses: Vec<HostStatus<'_>> = reports
        .iter()
        .map(|Row { machine, report }| {
            let machine = machine.as_ref().map(MachineName::as_str);
            let label = |host: &str| render::host_label(host, machine);
            match report {
                HostReport::Answering(snapshot) => HostStatus::Answering(snapshot, machine),
                HostReport::NotSharing { host } => HostStatus::NotSharing { host: label(host) },
                HostReport::Unreachable { host } => HostStatus::Unreachable { host: label(host) },
                HostReport::Throttled { host } => HostStatus::Throttled { host: label(host) },
                HostReport::Unreadable { host, reason } => HostStatus::Unreadable {
                    host: label(host),
                    reason: reason.clone(),
                },
            }
        })
        .collect();
    render::render_answer(&statuses, projects, prs, now, color, partial)
}

/// Everything the query produced, held until the progress line is gone.
struct Gathered {
    reports: Vec<Row>,
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

    fn rows(reports: Vec<HostReport>) -> Vec<Row> {
        reports.into_iter().map(Row::from).collect()
    }

    fn machine(name: &str) -> MachineName {
        MachineName::try_from(name).unwrap()
    }

    fn now() -> Timestamp {
        "2026-08-10T19:57:01.322Z".parse().unwrap()
    }

    #[test]
    fn a_machine_report_keeps_its_machine_and_its_attribution_check() {
        let id = QueryId(1);
        let mut collected = Collected::default();
        for incoming in [
            frame(ServerFrame::MachineReport {
                id,
                host: host("sandbank"),
                machine: Some(machine("laptop")),
                state: HostState::Answering {
                    snapshot: snapshot("sandbank"),
                },
            }),
            frame(ServerFrame::MachineReport {
                id,
                host: host("sandbank"),
                machine: Some(machine("desktop")),
                state: HostState::Unreachable {},
            }),
            // Attributed to `shelter`, claiming to be `sandbank` (rule 21).
            frame(ServerFrame::MachineReport {
                id,
                host: host("shelter"),
                machine: Some(machine("laptop")),
                state: HostState::Answering {
                    snapshot: snapshot("sandbank"),
                },
            }),
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

        let out = present(
            &collected.reports,
            &[],
            &PrStatuses::new(),
            now(),
            false,
            false,
        );
        assert!(out.contains("sandbank@laptop — 0 agents"), "{out}");
        assert!(out.contains("sandbank@desktop — unreachable"), "{out}");
        assert!(out.contains("shelter@laptop — unreadable"), "{out}");
        assert!(!out.contains("sandbank@laptop — unreadable"), "{out}");
    }

    #[test]
    fn a_row_without_a_machine_reads_exactly_as_before() {
        let out = present(
            &rows(vec![
                HostReport::Answering(snapshot("sandbank")),
                HostReport::Unreachable {
                    host: "shelter".to_owned(),
                },
            ]),
            &[],
            &PrStatuses::new(),
            now(),
            false,
            false,
        );
        assert!(out.contains("sandbank — 0 agents"), "{out}");
        assert!(out.contains("shelter — unreachable"), "{out}");
        assert!(!out.contains('@'), "{out}");
    }

    /// A router on a local socket. `answer` gets each frame the reader sends,
    /// decoded, and returns what to send back.
    async fn fake_router(
        answer: impl Fn(ClientFrame) -> Vec<ServerFrame> + Send + 'static,
    ) -> client::Socket {
        use futures_util::{SinkExt as _, StreamExt as _};
        use tokio_tungstenite::tungstenite::Message;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(Ok(Message::Text(text))) = ws.next().await {
                // What a router does with a frame it cannot decode: nothing.
                let Ok(frame) = sealmesh_proto::decode::<ClientFrame>(text.as_bytes()) else {
                    continue;
                };
                for reply in answer(frame) {
                    let text = serde_json::to_string(&reply).unwrap();
                    if ws.send(Message::Text(text.into())).await.is_err() {
                        return;
                    }
                }
            }
        });
        tokio_tungstenite::connect_async_with_config(format!("ws://{addr}"), None, false)
            .await
            .unwrap()
            .0
    }

    fn map() -> Vec<ProjectEntry> {
        vec![ProjectEntry {
            id: sealmesh_wire::ProjectId::try_from("sealmesh").unwrap(),
            repos: vec![RepoSlug::try_from("example-org/sealmesh").unwrap()],
        }]
    }

    fn counter() -> std::sync::Arc<std::sync::atomic::AtomicUsize> {
        std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0))
    }

    #[tokio::test]
    async fn a_router_that_knows_machines_is_asked_once_and_names_each_machine() {
        let asks = counter();
        let counted = asks.clone();
        let socket = fake_router(move |frame| match frame {
            ClientFrame::AskMachines { id } => {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                vec![
                    ServerFrame::MachineReport {
                        id,
                        host: host("sandbank"),
                        machine: Some(machine("desktop")),
                        state: HostState::NotSharing {},
                    },
                    ServerFrame::MachineReport {
                        id,
                        host: host("sandbank"),
                        machine: Some(machine("laptop")),
                        state: HostState::Answering {
                            snapshot: snapshot("sandbank"),
                        },
                    },
                    ServerFrame::Complete { id },
                ]
            }
            ClientFrame::Projects {} => vec![ServerFrame::Projects { projects: map() }],
            ClientFrame::Ask { .. } => {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Vec::new()
            }
            _ => Vec::new(),
        })
        .await;

        let answers = ask(socket, |_| ControlFlow::Continue(())).await.unwrap();
        assert_eq!(
            answers.projects,
            map(),
            "the map still arrives, after the rows"
        );
        let machines: Vec<Option<&str>> = answers
            .reports
            .iter()
            .map(|row| row.machine.as_ref().map(MachineName::as_str))
            .collect();
        assert_eq!(machines, vec![Some("desktop"), Some("laptop")]);
        assert_eq!(
            asks.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "one question, one fan-out"
        );
    }

    #[tokio::test]
    async fn a_router_from_before_machines_is_asked_again_the_old_way() {
        // A v0.1.0 router: it cannot decode `AskMachines`, so it skips it and
        // answers the `Projects` behind it first.
        let asks = counter();
        let counted = asks.clone();
        let socket = fake_router(move |frame| match frame {
            ClientFrame::Ask { id } => {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                vec![
                    ServerFrame::Report {
                        id,
                        host: host("sandbank"),
                        state: HostState::Answering {
                            snapshot: snapshot("sandbank"),
                        },
                    },
                    ServerFrame::Report {
                        id,
                        host: host("sandbank"),
                        state: HostState::NotSharing {},
                    },
                    ServerFrame::Complete { id },
                ]
            }
            ClientFrame::Projects {} => vec![ServerFrame::Projects { projects: map() }],
            _ => Vec::new(),
        })
        .await;

        let answers = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            ask(socket, |_| ControlFlow::Continue(())),
        )
        .await
        .expect("the reader waited on a question the router never heard")
        .unwrap();
        assert_eq!(answers.projects, map());
        assert_eq!(answers.reports.len(), 2, "the host once per machine");
        assert!(answers.reports.iter().all(|row| row.machine.is_none()));
        assert_eq!(asks.load(std::sync::atomic::Ordering::SeqCst), 1);
        let out = present(
            &answers.reports,
            &answers.projects,
            &PrStatuses::new(),
            now(),
            false,
            false,
        );
        assert!(out.contains("sandbank — 0 agents"), "{out}");
        assert!(out.contains("sandbank — not sharing"), "{out}");
    }

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
            &[report.into()],
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
        let reports = rows(vec![
            HostReport::Answering(snapshot("sandbank")),
            HostReport::Unreadable {
                host: "shelter".to_owned(),
                reason: "answer is not valid json for this schema (line 1, column 4)".to_owned(),
            },
        ]);
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
        let reports = rows(vec![interpret(
            host("sandbank"),
            HostState::Answering {
                snapshot: snapshot("sandbank"),
            },
        )]);
        let now = "2026-08-10T19:57:01.322Z".parse().unwrap();

        let partial = present(&reports, &[], &PrStatuses::new(), now, false, true);
        assert!(partial.contains("sandbank"), "{partial}");
        assert!(partial.contains("this answer is partial"), "{partial}");

        let complete = present(&reports, &[], &PrStatuses::new(), now, false, false);
        assert!(!complete.contains("partial"), "{complete}");
    }

    #[test]
    fn the_transport_states_map_to_distinct_rows() {
        let reports = rows(vec![
            interpret(host("a"), HostState::Unreachable {}),
            interpret(host("b"), HostState::NotSharing {}),
        ]);
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
            &[report.into()],
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
