use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use sealmesh_proto::{HostState, QueryId, ServerFrame};
use sealmesh_wire::HostId;
use tokio::sync::{mpsc, oneshot};

const DAEMON_QUEUE: usize = 8;

pub type Outbox = mpsc::Sender<ServerFrame>;

struct Daemon {
    outbox: Outbox,
    epoch: u64,
}

#[derive(Default)]
struct Inner {
    live: HashMap<HostId, Daemon>,
    seen: BTreeSet<HostId>,
    pending: HashMap<QueryId, oneshot::Sender<HostState>>,
}

pub struct Registry {
    inner: Mutex<Inner>,
    next_ticket: AtomicU64,
    next_epoch: AtomicU64,
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
            next_epoch: AtomicU64::new(1),
        }
    }

    pub fn attach(&self, host: HostId) -> (mpsc::Receiver<ServerFrame>, Attached<'_>) {
        let (tx, rx) = mpsc::channel(DAEMON_QUEUE);
        let epoch = self.next_epoch.fetch_add(1, Ordering::Relaxed);

        if let Ok(mut inner) = self.inner.lock() {
            inner.seen.insert(host.clone());
            inner
                .live
                .insert(host.clone(), Daemon { outbox: tx, epoch });
        }

        (
            rx,
            Attached {
                registry: self,
                host,
                epoch,
            },
        )
    }

    pub fn known_hosts(&self) -> Vec<HostId> {
        match self.inner.lock() {
            Ok(inner) => inner.seen.iter().cloned().collect(),
            Err(_) => Vec::new(),
        }
    }

    pub fn dispatch(
        &self,
        host: &HostId,
        asked_by: &HostId,
    ) -> Option<(QueryId, oneshot::Receiver<HostState>)> {
        let ticket = QueryId(self.next_ticket.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = oneshot::channel();

        let mut inner = self.inner.lock().ok()?;
        let daemon = inner.live.get(host)?;
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

    fn detach(&self, host: &HostId, epoch: u64) {
        if let Ok(mut inner) = self.inner.lock()
            && inner.live.get(host).is_some_and(|d| d.epoch == epoch)
        {
            inner.live.remove(host);
        }
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

pub struct Attached<'a> {
    registry: &'a Registry,
    host: HostId,
    epoch: u64,
}

impl Attached<'_> {
    pub fn host(&self) -> &HostId {
        &self.host
    }
}

impl Drop for Attached<'_> {
    fn drop(&mut self) {
        self.registry.detach(&self.host, self.epoch);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn host(name: &str) -> HostId {
        HostId::try_from(name).unwrap()
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
    async fn a_reconnect_replaces_the_old_connection_without_the_old_guard_killing_it() {
        let registry = Registry::new();
        let (_rx_old, guard_old) = registry.attach(host("sandbank"));
        let (_rx_new, _guard_new) = registry.attach(host("sandbank"));

        assert_eq!(registry.live_count(), 1, "one host, one live connection");
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
