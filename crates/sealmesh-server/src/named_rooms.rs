//! Named rooms: `[[room]] name = "ops" members = ["claude", "grokbot"]`.
//!
//! A std or otw router is handed this table by whoever embeds it (the daemon,
//! from its own config). A cloud router never is, so its table is empty and
//! every `Scope::Named` join is refused before anything is asked of GitHub.
//!
//! Membership is a closed list of identity labels. Today the label a
//! connection is checked against is its host (the login the directory
//! returns, folded by `host_from_login`). The std PR stamps an agent name on
//! each local connection; `room_access` is the one place that picks the label,
//! so that is the seam to change.

use std::collections::{BTreeMap, BTreeSet};

use sealmesh_chat_wire::RoomName;
use sealmesh_wire::HostId;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NamedRooms {
    rooms: BTreeMap<RoomName, BTreeSet<HostId>>,
}

impl NamedRooms {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `name` with `members`. A second insert of one name replaces the
    /// first; the daemon's config refuses a duplicate before it gets here.
    pub fn insert(&mut self, name: RoomName, members: impl IntoIterator<Item = HostId>) {
        self.rooms.insert(name, members.into_iter().collect());
    }

    /// The members of `name`, or `None` for a room this router was not
    /// configured with.
    pub fn members_of(&self, name: &RoomName) -> Option<&BTreeSet<HostId>> {
        self.rooms.get(name)
    }

    /// Whether `identity` is listed in `name`. False for an unknown room.
    pub fn admits(&self, name: &RoomName, identity: &HostId) -> bool {
        self.members_of(name)
            .is_some_and(|members| members.contains(identity))
    }

    pub fn len(&self) -> usize {
        self.rooms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rooms.is_empty()
    }
}

impl FromIterator<(RoomName, Vec<HostId>)> for NamedRooms {
    fn from_iter<I: IntoIterator<Item = (RoomName, Vec<HostId>)>>(iter: I) -> Self {
        let mut rooms = Self::new();
        for (name, members) in iter {
            rooms.insert(name, members);
        }
        rooms
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn name(s: &str) -> RoomName {
        RoomName::try_from(s).unwrap()
    }

    fn host(s: &str) -> HostId {
        HostId::try_from(s).unwrap()
    }

    #[test]
    fn only_listed_members_of_a_known_room_are_admitted() {
        let rooms: NamedRooms = [(name("ops"), vec![host("claude"), host("grokbot")])]
            .into_iter()
            .collect();
        assert!(rooms.admits(&name("ops"), &host("claude")));
        assert!(rooms.admits(&name("ops"), &host("grokbot")));
        assert!(!rooms.admits(&name("ops"), &host("mallory")));
        assert!(!rooms.admits(&name("dev"), &host("claude")));
        assert!(NamedRooms::new().is_empty());
    }
}
