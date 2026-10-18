use std::collections::BTreeMap;

use jiff::Timestamp;

use crate::wire::{self, EVENT_SCHEMA_VERSION, Event, EventKind, HostId, JobId};

#[derive(Debug, Default)]
pub struct Watch {
    previous: Option<Vec<wire::Agent>>,
}

impl Watch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(
        &mut self,
        agents: Vec<wire::Agent>,
        engineer: &HostId,
        at: Timestamp,
    ) -> Vec<Event> {
        let events = match self.previous.as_deref() {
            Some(previous) => transitions(previous, &agents, engineer, at),
            None => Vec::new(),
        };
        self.previous = Some(agents);
        events
    }

    pub fn forget(&mut self) {
        self.previous = None;
    }

    pub fn is_watching(&self) -> bool {
        self.previous.is_some()
    }
}

pub fn transitions(
    previous: &[wire::Agent],
    current: &[wire::Agent],
    engineer: &HostId,
    at: Timestamp,
) -> Vec<Event> {
    // Keyed on (tool, id) for the same reason the collector's merge is: two
    // tools can now share an id in one snapshot, and a plain `id` key would
    // silently keep the last one — dropping the other agent from the diff and
    // flapping Started/Finished for it on every cycle.
    type Key<'a> = (&'a Option<wire::ToolId>, &'a JobId);
    let before: BTreeMap<Key<'_>, &wire::Agent> =
        previous.iter().map(|a| ((&a.tool, &a.id), a)).collect();
    let after: BTreeMap<Key<'_>, &wire::Agent> =
        current.iter().map(|a| ((&a.tool, &a.id), a)).collect();

    let mut ids: Vec<Key<'_>> = before.keys().chain(after.keys()).copied().collect();
    ids.sort_unstable();
    ids.dedup();

    let mut events = Vec::new();
    for id in ids {
        match (before.get(&id), after.get(&id)) {
            (None, Some(now)) => events.push(event(now, EventKind::Started, engineer, at)),
            (Some(was), None) => events.push(event(was, EventKind::Finished, engineer, at)),
            (Some(was), Some(now)) => {
                if was.run_state != now.run_state {
                    events.push(event(
                        now,
                        EventKind::StateChanged {
                            from: was.run_state,
                            to: now.run_state,
                        },
                        engineer,
                        at,
                    ));
                }
                if was.pr.is_none() && now.pr.is_some() {
                    events.push(event(now, EventKind::PrOpened, engineer, at));
                }
            }
            (None, None) => {}
        }
    }
    events
}

fn event(agent: &wire::Agent, kind: EventKind, engineer: &HostId, at: Timestamp) -> Event {
    Event {
        schema: EVENT_SCHEMA_VERSION,
        at,
        engineer: engineer.clone(),
        repo: agent.repo.clone(),
        job: agent.id.clone(),
        kind,
        ticket: agent.ticket.clone(),
        branch: agent.branch.clone(),
        pr: agent.pr,
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

    use super::*;

    use crate::wire::{BranchName, RepoSlug, RunState, TicketId};

    fn engineer() -> HostId {
        HostId::try_from("sandbank").unwrap()
    }

    fn at() -> Timestamp {
        "2026-08-10T19:56:51.322Z".parse().unwrap()
    }

    fn agent(id: &str, run_state: RunState, pr: Option<u32>) -> wire::Agent {
        wire::Agent {
            id: JobId::try_from(id).unwrap(),
            repo: RepoSlug::try_from("example-org/example-repo").unwrap(),
            run_state,
            branch: Some(BranchName::try_from("feature/eng-557-qa-rescore-ui").unwrap()),
            ticket: Some(TicketId::new("ENG", 557).unwrap()),
            ahead: Some(3),
            dirty_files: Some(7),
            tasks: None,
            pr,
            tool: None,
            started_at: "2026-08-10T17:41:03.591Z".parse().unwrap(),
            updated_at: "2026-08-10T19:53:28.829Z".parse().unwrap(),
        }
    }

    fn diff(previous: &[wire::Agent], current: &[wire::Agent]) -> Vec<Event> {
        transitions(previous, current, &engineer(), at())
    }

    #[test]
    fn a_job_appearing_is_exactly_one_started() {
        let now = [agent("1e631e34", RunState::Working, None)];
        let events = diff(&[], &now);
        assert_eq!(events.len(), 1, "{events:#?}");
        assert_eq!(events[0].kind, EventKind::Started);
        assert_eq!(events[0].job.as_str(), "1e631e34");
        assert_eq!(events[0].engineer, engineer());
        assert_eq!(events[0].at, at());
    }

    #[test]
    fn a_job_disappearing_is_exactly_one_finished() {
        let was = [agent("1e631e34", RunState::Working, None)];
        let events = diff(&was, &[]);
        assert_eq!(events.len(), 1, "{events:#?}");
        assert_eq!(events[0].kind, EventKind::Finished);
        assert_eq!(events[0].job.as_str(), "1e631e34");
    }

    #[test]
    fn a_changed_run_state_carries_both_halves() {
        let was = [agent("1e631e34", RunState::Working, None)];
        let now = [agent("1e631e34", RunState::NeedsInput, None)];
        let events = diff(&was, &now);
        assert_eq!(events.len(), 1, "{events:#?}");
        assert_eq!(
            events[0].kind,
            EventKind::StateChanged {
                from: RunState::Working,
                to: RunState::NeedsInput,
            }
        );
    }

    #[test]
    fn a_pr_appearing_is_exactly_one_pr_opened() {
        let was = [agent("1e631e34", RunState::Working, None)];
        let now = [agent("1e631e34", RunState::Working, Some(627))];
        let events = diff(&was, &now);
        assert_eq!(events.len(), 1, "{events:#?}");
        assert_eq!(events[0].kind, EventKind::PrOpened);
        assert_eq!(events[0].pr, Some(627));
    }

    #[test]
    fn an_unchanged_collection_produces_nothing() {
        let same = [
            agent("1e631e34", RunState::Working, Some(627)),
            agent("949c7c62", RunState::Idle, None),
        ];
        assert!(diff(&same, &same).is_empty());
        assert!(diff(&[], &[]).is_empty());
    }

    #[test]
    fn a_job_first_seen_with_a_pr_reports_only_that_it_started() {
        let now = [agent("1e631e34", RunState::Working, Some(627))];
        let events = diff(&[], &now);
        assert_eq!(events.len(), 1, "{events:#?}");
        assert_eq!(events[0].kind, EventKind::Started);
        assert_eq!(events[0].pr, Some(627));
    }

    #[test]
    fn a_pr_that_was_already_there_never_reopens() {
        let was = [agent("1e631e34", RunState::Working, Some(627))];
        let now = [agent("1e631e34", RunState::Working, Some(628))];
        assert!(diff(&was, &now).is_empty());
    }

    #[test]
    fn a_job_that_changed_state_and_gained_a_pr_reports_both() {
        let was = [agent("1e631e34", RunState::Working, None)];
        let now = [agent("1e631e34", RunState::NeedsInput, Some(627))];
        let kinds: Vec<_> = diff(&was, &now).into_iter().map(|e| e.kind).collect();
        assert_eq!(
            kinds,
            vec![
                EventKind::StateChanged {
                    from: RunState::Working,
                    to: RunState::NeedsInput,
                },
                EventKind::PrOpened,
            ]
        );
    }

    #[test]
    fn the_order_of_the_collection_does_not_change_the_batch() {
        let was = [agent("949c7c62", RunState::Working, None)];
        let forward = [
            agent("1e631e34", RunState::Working, None),
            agent("aaaa1111", RunState::Working, None),
        ];
        let backward = [
            agent("aaaa1111", RunState::Working, None),
            agent("1e631e34", RunState::Working, None),
        ];
        assert_eq!(diff(&was, &forward), diff(&was, &backward));
        let ids: Vec<_> = diff(&was, &forward)
            .iter()
            .map(|e| e.job.as_str().to_owned())
            .collect();
        assert_eq!(ids, vec!["1e631e34", "949c7c62", "aaaa1111"]);
    }

    #[test]
    fn the_first_observation_is_a_baseline_and_reports_nothing() {
        let mut watch = Watch::new();
        assert!(!watch.is_watching());

        let running = vec![agent("1e631e34", RunState::Working, None)];
        assert!(watch.observe(running, &engineer(), at()).is_empty());
        assert!(watch.is_watching());

        let changed = vec![agent("1e631e34", RunState::NeedsInput, None)];
        let events = watch.observe(changed, &engineer(), at());
        assert_eq!(events.len(), 1, "{events:#?}");
    }

    #[test]
    fn an_empty_collection_is_an_observation_not_a_gap() {
        let mut watch = Watch::new();
        watch.observe(
            vec![agent("1e631e34", RunState::Working, None)],
            &engineer(),
            at(),
        );
        let events = watch.observe(Vec::new(), &engineer(), at());
        assert_eq!(events.len(), 1, "{events:#?}");
        assert_eq!(events[0].kind, EventKind::Finished);
    }

    #[test]
    fn forgetting_the_baseline_makes_the_off_period_unreportable() {
        let mut watch = Watch::new();
        watch.observe(
            vec![agent("1e631e34", RunState::Working, None)],
            &engineer(),
            at(),
        );

        watch.forget();
        assert!(!watch.is_watching());

        let after_the_off_period = vec![
            agent("1e631e34", RunState::Done, Some(627)),
            agent("bbbb2222", RunState::Working, None),
        ];
        assert!(
            watch
                .observe(after_the_off_period, &engineer(), at())
                .is_empty()
        );
    }
}
