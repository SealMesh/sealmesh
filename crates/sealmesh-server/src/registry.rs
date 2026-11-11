//! Every attached daemon connection, and the queries waiting on them.
//!
//! A host is a GitHub login, so one person's two laptops are one host. The
//! registry therefore keys connections, not hosts: each connection gets its
//! own slot, and a host has as many rows in a fan-out as it has connections.
//!
//! A daemon names its machine right after its handshake
//! (`ClientFrame::Machine`). A connection that names a machine the host
//! already has connected replaces that connection — a laptop back from sleep
//! takes the place of its own dead socket — while another machine is added
//! beside it. A daemon that never names one (a build from before machine
//! names) keeps an anonymous slot until its socket closes.
//!
//! Replacing every connection of a host with the newest one, which is what
//! this used to do, made two machines of one login evict each other on every
//! reconnect, so they took turns being connected.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use sealmesh_proto::{HostState, MachineName, QueryId, ServerFrame};
use sealmesh_wire::HostId;
use tokio::sync::{mpsc, oneshot};

const DAEMON_QUEUE: usize = 8;

/// Most connections one host may hold at once. Each one is asked on every
/// fan-out, so without a bound one login could make every `who` wait on as
/// many sockets as it cares to open. Past it, the host's oldest connection is
/// dropped: the newest is the one most likely to be alive, and a person only
/// ever crowds out their own machines.
pub const MAX_CONNECTIONS_PER_HOST: usize = 8;

/// Most machine names remembered per host after they disconnect, so a closed
/// laptop still shows as unreachable beside the one that is open. Bounded for
/// the same reason as [`MAX_CONNECTIONS_PER_HOST`]: the name is the daemon's
/// to choose. The least recently connected is forgotten first.
pub const MAX_REMEMBERED_MACHINES: usize = 8;

pub type Outbox = mpsc::Sender<ServerFrame>;

/// Whose answer a row is: the host the router attributed it to (rule 12), and
/// which of that host's machines, when the daemon named one.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Attributed {
    pub host: HostId,
    pub machine: Option<MachineName>,
}

impl Attributed {
    pub fn host(host: HostId) -> Self {
        Self {
            host,
            machine: None,
        }
    }
}

/// One daemon connection. Issued by the registry and never reused within a
/// router process, so an older connection can never be mistaken for the one
/// that replaced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnId(u64);

/// One row of a fan-out, before anyone is asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// A connection to ask.
    Live(ConnId, Attributed),
    /// Nothing to ask: a host with no connection, or a named machine of a host
    /// that has since disconnected. Its row is `Unreachable`.
    Gone(Attributed),
}

struct Daemon {
    host: HostId,
    machine: Option<MachineName>,
    outbox: Outbox,
}

#[derive(Default)]
struct Inner {
    /// Ordered by `ConnId`, so a host's oldest connection is its first.
    live: BTreeMap<ConnId, Daemon>,
    seen: BTreeSet<HostId>,
    /// Machine names each host has connected from, least recent first.
    machines: HashMap<HostId, VecDeque<MachineName>>,
    pending: HashMap<QueryId, oneshot::Sender<HostState>>,
}

impl Inner {
    fn connections_of<'a>(&'a self, host: &'a HostId) -> impl Iterator<Item = ConnId> + 'a {
        self.live
            .iter()
            .filter(move |(_, daemon)| daemon.host == *host)
            .map(|(conn, _)| *conn)
    }
}

pub struct Registry {
    inner: Mutex<Inner>,
    next_ticket: AtomicU64,
    next_conn: AtomicU64,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            next_ticket: AtomicU64::new(1),
            next_conn: AtomicU64::new(1),
        }
    }

    /// Adds one connection for `host`, in an anonymous slot until it names its
    /// machine. Every other connection the host holds stays, unless the host is
    /// already at [`MAX_CONNECTIONS_PER_HOST`], when its oldest is dropped.
    pub fn attach(&self, host: HostId) -> (mpsc::Receiver<ServerFrame>, Attached<'_>) {
        let (tx, rx) = mpsc::channel(DAEMON_QUEUE);
        let conn = ConnId(self.next_conn.fetch_add(1, Ordering::Relaxed));

        if let Ok(mut inner) = self.inner.lock() {
            inner.seen.insert(host.clone());
            let held: Vec<ConnId> = inner.connections_of(&host).collect();
            let excess = held
                .len()
                .saturating_add(1)
                .saturating_sub(MAX_CONNECTIONS_PER_HOST);
            for oldest in held.into_iter().take(excess) {
                // Dropping its outbox is what ends that connection's task.
                inner.live.remove(&oldest);
            }
            inner.live.insert(
                conn,
                Daemon {
                    host: host.clone(),
                    machine: None,
                    outbox: tx,
                },
            );
        }

        (
            rx,
            Attached {
                registry: self,
                host,
                conn,
            },
        )
    }

    /// Names the machine connection `conn` comes from, and drops any other
    /// connection of the same host that named the same machine. Returns how
    /// many it dropped, or `None` when `conn` is no longer attached or has
    /// already named a machine — a name is set once per connection.
    fn name(&self, conn: ConnId, machine: MachineName) -> Option<usize> {
        let mut inner = self.inner.lock().ok()?;
        let daemon = inner.live.get_mut(&conn)?;
        if daemon.machine.is_some() {
            return None;
        }
        daemon.machine = Some(machine.clone());
        let host = daemon.host.clone();

        let replaced: Vec<ConnId> = inner
            .live
            .iter()
            .filter(|(other, daemon)| {
                **other != conn && daemon.host == host && daemon.machine.as_ref() == Some(&machine)
            })
            .map(|(other, _)| *other)
            .collect();
        for other in &replaced {
            inner.live.remove(other);
        }

        let remembered = inner.machines.entry(host).or_default();
        remembered.retain(|known| *known != machine);
        remembered.push_back(machine);
        while remembered.len() > MAX_REMEMBERED_MACHINES {
            remembered.pop_front();
        }

        Some(replaced.len())
    }

    pub fn known_hosts(&self) -> Vec<HostId> {
        match self.inner.lock() {
            Ok(inner) => inner.seen.iter().cloned().collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Every row one fan-out has, in order: each host, then within it each
    /// live connection, then each remembered machine with no live connection.
    /// A host with neither has one anonymous `Gone` row, exactly as it did
    /// when a host had one connection at most.
    pub fn targets(&self) -> Vec<Target> {
        let Ok(inner) = self.inner.lock() else {
            return Vec::new();
        };
        let mut by_host: BTreeMap<&HostId, Vec<(ConnId, Option<&MachineName>)>> = BTreeMap::new();
        for (conn, daemon) in &inner.live {
            by_host
                .entry(&daemon.host)
                .or_default()
                .push((*conn, daemon.machine.as_ref()));
        }

        let mut targets = Vec::new();
        for host in &inner.seen {
            let live = by_host.get(host).map(Vec::as_slice).unwrap_or_default();
            for (conn, machine) in live {
                targets.push(Target::Live(
                    *conn,
                    Attributed {
                        host: host.clone(),
                        machine: machine.cloned(),
                    },
                ));
            }
            let gone = inner
                .machines
                .get(host)
                .into_iter()
                .flatten()
                .filter(|known| !live.iter().any(|(_, machine)| *machine == Some(*known)))
                .collect::<Vec<&MachineName>>();
            let before = targets.len();
            for machine in gone {
                targets.push(Target::Gone(Attributed {
                    host: host.clone(),
                    machine: Some(machine.clone()),
                }));
            }
            if live.is_empty() && targets.len() == before {
                targets.push(Target::Gone(Attributed::host(host.clone())));
            }
        }
        targets
    }

    /// Sends one query to connection `conn`. `None` when it is gone or its
    /// queue is full, which the fan-out reports as `Unreachable`.
    pub fn dispatch_to(
        &self,
        conn: ConnId,
        asked_by: &HostId,
    ) -> Option<(QueryId, oneshot::Receiver<HostState>)> {
        let ticket = QueryId(self.next_ticket.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = oneshot::channel();

        let mut inner = self.inner.lock().ok()?;
        let daemon = inner.live.get(&conn)?;
        if daemon
            .outbox
            .try_send(ServerFrame::Query {
                id: ticket,
                asked_by: asked_by.clone(),
            })
            .is_err()
        {
            return None;
        }
        inner.pending.insert(ticket, tx);
        Some((ticket, rx))
    }

    pub fn resolve(&self, ticket: QueryId, state: HostState) {
        if let Ok(mut inner) = self.inner.lock()
            && let Some(waiting) = inner.pending.remove(&ticket)
        {
            let _ = waiting.send(state);
        }
    }

    pub fn forget(&self, ticket: QueryId) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.pending.remove(&ticket);
        }
    }

    fn detach(&self, conn: ConnId) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.live.remove(&conn);
        }
    }

    /// Sends one query to `host`'s newest connection.
    #[cfg(test)]
    fn dispatch(
        &self,
        host: &HostId,
        asked_by: &HostId,
    ) -> Option<(QueryId, oneshot::Receiver<HostState>)> {
        let newest = self.inner.lock().ok()?.connections_of(host).last()?;
        self.dispatch_to(newest, asked_by)
    }

    #[cfg(test)]
    fn live_count(&self) -> usize {
        self.inner.lock().map(|i| i.live.len()).unwrap_or_default()
    }

    #[cfg(test)]
    fn pending_count(&self) -> usize {
        self.inner
            .lock()
            .map(|i| i.pending.len())
            .unwrap_or_default()
    }
}

/// One attached connection. Dropping it takes the connection out of the
/// registry — unless something already did, such as a newer connection from
/// the same machine, in which case it touches nothing.
pub struct Attached<'a> {
    registry: &'a Registry,
    host: HostId,
    conn: ConnId,
}

impl Attached<'_> {
    pub fn host(&self) -> &HostId {
        &self.host
    }

    /// Names this connection's machine. See [`Registry::name`].
    pub fn name(&self, machine: MachineName) -> Option<usize> {
        self.registry.name(self.conn, machine)
    }
}

impl Drop for Attached<'_> {
    fn drop(&mut self) {
        self.registry.detach(self.conn);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn host(name: &str) -> HostId {
        HostId::try_from(name).unwrap()
    }

    fn machine(name: &str) -> MachineName {
        MachineName::try_from(name).unwrap()
    }

    fn on(host_name: &str, machine_name: Option<&str>) -> Attributed {
        Attributed {
            host: host(host_name),
            machine: machine_name.map(machine),
        }
    }

    /// Each target as (attributed, whether it is live).
    fn rows(registry: &Registry) -> Vec<(Attributed, bool)> {
        registry
            .targets()
            .into_iter()
            .map(|target| match target {
                Target::Live(_, who) => (who, true),
                Target::Gone(who) => (who, false),
            })
            .collect()
    }

    #[tokio::test]
    async fn a_dispatched_query_reaches_the_daemon_and_its_answer_comes_back() {
        let registry = Registry::new();
        let (mut rx, _guard) = registry.attach(host("sandbank"));

        let (_ticket, answer) = registry
            .dispatch(&host("sandbank"), &host("reader"))
            .unwrap();
        let Some(ServerFrame::Query { id, .. }) = rx.recv().await else {
            panic!("daemon received no query");
        };

        registry.resolve(id, HostState::NotSharing {});
        assert_eq!(answer.await.unwrap(), HostState::NotSharing {});
    }

    #[tokio::test]
    async fn a_host_with_no_daemon_cannot_be_dispatched_to() {
        let registry = Registry::new();
        assert!(
            registry
                .dispatch(&host("nobody"), &host("reader"))
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_reconnect_from_the_same_machine_replaces_the_old_connection() {
        let registry = Registry::new();
        let (mut rx_old, guard_old) = registry.attach(host("sandbank"));
        assert_eq!(guard_old.name(machine("laptop")), Some(0));
        let (_rx_new, guard_new) = registry.attach(host("sandbank"));
        assert_eq!(
            guard_new.name(machine("laptop")),
            Some(1),
            "the same machine's older connection is the one replaced"
        );

        assert_eq!(registry.live_count(), 1, "one machine, one live connection");
        assert!(
            rx_old.recv().await.is_none(),
            "the replaced connection's queue closes, which ends its task"
        );
        drop(guard_old);
        assert_eq!(
            registry.live_count(),
            1,
            "the superseded guard must not detach the connection that replaced it"
        );
        assert!(
            registry
                .dispatch(&host("sandbank"), &host("reader"))
                .is_some()
        );
    }

    #[tokio::test]
    async fn two_machines_of_one_host_both_stay_attached() {
        let registry = Registry::new();
        let (_rx_a, a) = registry.attach(host("sandbank"));
        assert_eq!(a.name(machine("laptop")), Some(0));
        let (_rx_b, b) = registry.attach(host("sandbank"));
        assert_eq!(b.name(machine("desktop")), Some(0));

        assert_eq!(registry.live_count(), 2);
        assert_eq!(
            rows(&registry),
            vec![
                (on("sandbank", Some("laptop")), true),
                (on("sandbank", Some("desktop")), true),
            ]
        );
    }

    #[tokio::test]
    async fn an_unnamed_connection_keeps_a_slot_of_its_own_beside_a_named_one() {
        let registry = Registry::new();
        let (_rx_old, _old) = registry.attach(host("sandbank"));
        let (_rx_new, new) = registry.attach(host("sandbank"));
        assert_eq!(new.name(machine("laptop")), Some(0));

        assert_eq!(
            rows(&registry),
            vec![
                (on("sandbank", None), true),
                (on("sandbank", Some("laptop")), true),
            ],
            "a daemon from before machine names is neither replaced nor replacing"
        );
    }

    #[tokio::test]
    async fn a_machine_is_named_once_per_connection() {
        let registry = Registry::new();
        let (_rx, guard) = registry.attach(host("sandbank"));
        assert_eq!(guard.name(machine("laptop")), Some(0));
        assert_eq!(guard.name(machine("desktop")), None);
        assert_eq!(
            rows(&registry),
            vec![(on("sandbank", Some("laptop")), true)]
        );
    }

    #[tokio::test]
    async fn a_disconnected_machine_is_still_listed_beside_the_live_one() {
        let registry = Registry::new();
        let (_rx_a, a) = registry.attach(host("sandbank"));
        a.name(machine("laptop"));
        let (_rx_b, b) = registry.attach(host("sandbank"));
        b.name(machine("desktop"));
        drop(a);

        assert_eq!(
            rows(&registry),
            vec![
                (on("sandbank", Some("desktop")), true),
                (on("sandbank", Some("laptop")), false),
            ]
        );
    }

    #[tokio::test]
    async fn a_host_with_nothing_connected_has_one_row() {
        let registry = Registry::new();
        let (_rx, guard) = registry.attach(host("sandbank"));
        drop(guard);
        assert_eq!(rows(&registry), vec![(on("sandbank", None), false)]);

        let (_rx, named) = registry.attach(host("shelter"));
        named.name(machine("laptop"));
        drop(named);
        assert_eq!(
            rows(&registry),
            vec![
                (on("sandbank", None), false),
                (on("shelter", Some("laptop")), false),
            ],
            "a named machine that left is its host's one row, not an extra anonymous one"
        );
    }

    #[tokio::test]
    async fn one_host_cannot_hold_more_than_its_share_of_connections() {
        let registry = Registry::new();
        let mut held = Vec::new();
        for _ in 0..MAX_CONNECTIONS_PER_HOST + 3 {
            held.push(registry.attach(host("sandbank")));
        }
        let (_rx, _other) = registry.attach(host("shelter"));
        assert_eq!(registry.live_count(), MAX_CONNECTIONS_PER_HOST + 1);

        // The oldest went, so the newest is still reachable.
        let (rx_newest, _) = held.last_mut().unwrap();
        assert!(
            registry
                .dispatch(&host("sandbank"), &host("reader"))
                .is_some()
        );
        assert!(rx_newest.recv().await.is_some());
        let (rx_oldest, _) = held.first_mut().unwrap();
        assert!(rx_oldest.recv().await.is_none());
    }

    #[tokio::test]
    async fn remembered_machines_are_bounded() {
        let registry = Registry::new();
        for n in 0..MAX_REMEMBERED_MACHINES + 4 {
            let (_rx, guard) = registry.attach(host("sandbank"));
            guard.name(machine(&format!("box-{n}")));
        }
        let listed = rows(&registry);
        assert_eq!(listed.len(), MAX_REMEMBERED_MACHINES);
        assert!(
            !listed.contains(&(on("sandbank", Some("box-0")), false)),
            "the least recent goes first: {listed:?}"
        );
    }

    #[tokio::test]
    async fn dropping_the_guard_detaches_the_host() {
        let registry = Registry::new();
        let (_rx, guard) = registry.attach(host("sandbank"));
        drop(guard);
        assert!(
            registry
                .dispatch(&host("sandbank"), &host("reader"))
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_disconnected_host_is_still_known() {
        let registry = Registry::new();
        let (_rx, guard) = registry.attach(host("sandbank"));
        drop(guard);
        assert_eq!(registry.known_hosts(), vec![host("sandbank")]);
    }

    #[tokio::test]
    async fn an_answer_for_an_unknown_ticket_is_dropped() {
        let registry = Registry::new();
        let (mut rx, _guard) = registry.attach(host("sandbank"));
        let (_ticket, answer) = registry
            .dispatch(&host("sandbank"), &host("reader"))
            .unwrap();
        let _ = rx.recv().await;

        registry.resolve(QueryId(999_999), HostState::NotSharing {});
        assert_eq!(
            registry.pending_count(),
            1,
            "a forged id must not resolve someone else's query"
        );
        drop(answer);
    }

    #[tokio::test]
    async fn a_daemon_that_stops_draining_reads_as_unreachable() {
        let registry = Registry::new();
        let (_rx, _guard) = registry.attach(host("sandbank"));

        for _ in 0..DAEMON_QUEUE {
            assert!(
                registry
                    .dispatch(&host("sandbank"), &host("reader"))
                    .is_some()
            );
        }
        assert!(
            registry
                .dispatch(&host("sandbank"), &host("reader"))
                .is_none(),
            "a full queue must read as unreachable, not block"
        );
    }

    #[tokio::test]
    async fn forgetting_a_query_releases_its_slot() {
        let registry = Registry::new();
        let (_rx, _guard) = registry.attach(host("sandbank"));
        let (ticket, _answer) = registry
            .dispatch(&host("sandbank"), &host("reader"))
            .unwrap();
        assert_eq!(registry.pending_count(), 1);

        registry.forget(ticket);
        assert_eq!(registry.pending_count(), 0);
    }
}
