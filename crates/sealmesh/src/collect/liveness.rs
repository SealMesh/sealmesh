use std::collections::BTreeMap;

const START_TOLERANCE_SECS: i64 = 60;

pub trait Liveness {
    fn started_at_secs(&self, pid: u32) -> Option<i64>;

    fn is(&self, pid: u32, started_at_ms: i64) -> bool {
        let Some(process_secs) = self.started_at_secs(pid) else {
            return false;
        };
        let declared_secs = started_at_ms / 1000;
        process_secs.saturating_sub(declared_secs).abs() <= START_TOLERANCE_SECS
    }
}

pub struct ProcessTable(BTreeMap<u32, i64>);

impl ProcessTable {
    pub fn read() -> Self {
        let mut system = sysinfo::System::new();
        system.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::All,
            true,
            sysinfo::ProcessRefreshKind::nothing(),
        );
        Self(
            system
                .processes()
                .iter()
                .map(|(pid, process)| {
                    (
                        pid.as_u32(),
                        i64::try_from(process.start_time()).unwrap_or(i64::MAX),
                    )
                })
                .collect(),
        )
    }
}

impl Liveness for ProcessTable {
    fn started_at_secs(&self, pid: u32) -> Option<i64> {
        self.0.get(&pid).copied()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    struct Fake(BTreeMap<u32, i64>);

    impl Liveness for Fake {
        fn started_at_secs(&self, pid: u32) -> Option<i64> {
            self.0.get(&pid).copied()
        }
    }

    fn table(entries: &[(u32, i64)]) -> Fake {
        Fake(entries.iter().copied().collect())
    }

    #[test]
    fn a_pid_that_does_not_exist_is_not_alive() {
        assert!(!table(&[]).is(64653, 1_787_510_714_656));
    }

    #[test]
    fn the_session_that_wrote_the_file_is_alive() {
        let procs = table(&[(64653, 1_787_510_713)]);
        assert!(procs.is(64653, 1_787_510_714_656));
    }

    #[test]
    fn a_reused_pid_is_not_the_process_that_wrote_the_file() {
        let procs = table(&[(64653, 1_787_510_713 + 3600)]);
        assert!(!procs.is(64653, 1_787_510_714_656));
    }

    #[test]
    fn a_slow_startup_inside_the_tolerance_is_still_alive() {
        for skew in [-START_TOLERANCE_SECS, -5, 0, 5, START_TOLERANCE_SECS] {
            let procs = table(&[(1, 1_000_000 + skew)]);
            assert!(procs.is(1, 1_000_000_000), "rejected a skew of {skew}s");
        }
    }

    #[test]
    fn just_past_the_tolerance_is_not_alive() {
        let procs = table(&[(1, 1_000_000 + START_TOLERANCE_SECS + 1)]);
        assert!(!procs.is(1, 1_000_000_000));
    }

    #[test]
    fn the_real_process_table_can_see_this_process() {
        let procs = ProcessTable::read();
        let me = std::process::id();
        let started = procs
            .started_at_secs(me)
            .expect("this process is not in its own process table");
        assert!(procs.is(me, started.saturating_mul(1000)));
    }
}
