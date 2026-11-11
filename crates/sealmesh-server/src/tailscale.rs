//! The otw-mode directory: identity from Tailscale, not from a token.
//!
//! An otw router runs on one of the user's machines and listens only on its
//! tailnet address. WireGuard has already authenticated whoever reaches that
//! address, so the router asks the local `tailscaled` who the peer is, through
//! the LocalAPI `whois` endpoint, and the `Hello` token (empty) is ignored.
//!
//! - The **host** is the person: the Tailscale user's `LoginName`, folded
//!   like a GitHub login ([`host_from_login`]). A tagged node has no user, so
//!   its host is the first of its tags the router allows.
//! - The **machine** is the Tailscale node's name, so two of one person's
//!   machines are two rows in the multi-machine registry and both stay
//!   connected.
//! - **Authorization** is the router's own allowlist on top of the tailnet
//!   ACL: a user in `allow_users`, or a node carrying a tag in `allow_tags`.
//!
//! Everything fails closed. A whois that errors, times out, answers anything
//! but 200, or answers something this module cannot parse is a `Denied`, and
//! so is a peer that is not a tailnet address. Daemons already back off and
//! retry, so a `tailscaled` restart costs one refused attempt, not an outage
//! that admits anyone.
//!
//! Wire format relied on (Tailscale `client/tailscale/apitype.WhoIsResponse`,
//! `tailcfg.Node`, `tailcfg.UserProfile`; Go field names, no JSON renames):
//!
//! ```json
//! { "Node": { "Name": "laptop.tail1234.ts.net.", "ComputedName": "laptop",
//!             "Tags": ["tag:sealmesh"] },
//!   "UserProfile": { "LoginName": "alice@github" } }
//! ```

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Mutex,
    time::Duration,
};

use sealmesh_proto::{MachineName, SecretToken, tailnet::is_tailnet_ip};
use sealmesh_wire::RepoSlug;
use serde::Deserialize;
use tokio::time::Instant;

use crate::auth::{Denied, Directory, PeerIdentity, RepoAccess, host_from_login};

/// How long one whois may take, connect to last byte.
pub const WHOIS_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a whois answer is trusted for a peer address. A tag or user
/// removed from the tailnet stops being admitted within this long.
pub const CACHE_TTL: Duration = Duration::from_secs(60);

/// Most peers remembered at once, for the cache and for the once-per-peer
/// log. A tailnet is small; past this the expired entries go, and if that is
/// not enough, everything does.
const MAX_REMEMBERED: usize = 1024;

/// Largest whois reply read. A real one is a few kilobytes.
const MAX_REPLY_BYTES: usize = 256 * 1024;

/// The `Host` header LocalAPI expects (`apitype.LocalAPIHost`).
const LOCALAPI_HOST: &str = "local-tailscaled.sock";

/// The `tailscaled` LocalAPI socket when none is configured: the open-source
/// daemon's default on this platform (`paths.DefaultTailscaledSocket`).
pub fn default_socket() -> PathBuf {
    sealmesh_proto::tailnet::default_tailscaled_socket()
}

/// `[otw]`'s allowlist. Both empty admits nobody, so it is refused at
/// startup rather than serving a router no one can use.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Allowlist {
    /// Tailscale login names, e.g. `alice@github`. Compared ignoring ASCII
    /// case.
    pub users: Vec<String>,
    /// ACL tags, e.g. `tag:sealmesh`. Compared exactly.
    pub tags: Vec<String>,
}

impl Allowlist {
    pub fn new(users: Vec<String>, tags: Vec<String>) -> anyhow::Result<Self> {
        let users: Vec<String> = users.into_iter().map(|u| u.trim().to_owned()).collect();
        let tags: Vec<String> = tags.into_iter().map(|t| t.trim().to_owned()).collect();
        if users.is_empty() && tags.is_empty() {
            anyhow::bail!(
                "[otw] allow_users and allow_tags are both empty, so nobody could connect. \
                 List the Tailscale logins (e.g. \"alice@github\") or ACL tags \
                 (e.g. \"tag:sealmesh\") this router admits"
            );
        }
        if let Some(blank) = users.iter().find(|u| u.is_empty()) {
            anyhow::bail!("[otw] allow_users has a blank entry {blank:?}");
        }
        for user in &users {
            host_from_login(user).map_err(|_| {
                anyhow::anyhow!(
                    "[otw] allow_users entry {user:?} does not fold into a sealmesh host name \
                     (at most 32 characters of [a-z0-9-] once folded)"
                )
            })?;
        }
        if let Some(bad) = tags
            .iter()
            .find(|t| t.strip_prefix("tag:").is_none_or(str::is_empty))
        {
            anyhow::bail!("[otw] allow_tags entry {bad:?} is not an ACL tag (tag:<name>)");
        }
        Ok(Self { users, tags })
    }

    fn admits_user(&self, login: &str) -> bool {
        self.users.iter().any(|u| u.eq_ignore_ascii_case(login))
    }
}

/// The parts of a whois reply this module reads. Unknown fields are ignored:
/// the reply carries much more than this, and grows between releases.
#[derive(Debug, Deserialize)]
struct WhoIsReply {
    #[serde(rename = "Node")]
    node: Option<WhoIsNode>,
    #[serde(rename = "UserProfile")]
    user: Option<WhoIsUser>,
}

#[derive(Debug, Deserialize)]
struct WhoIsNode {
    /// The node's FQDN, with a trailing dot.
    #[serde(rename = "Name", default)]
    name: String,
    /// Its MagicDNS base name. `omitzero`, so it may be absent.
    #[serde(rename = "ComputedName", default)]
    computed_name: String,
    /// ACL tags. A tagged node is owned by its tags, not by a user.
    #[serde(rename = "Tags", default)]
    tags: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct WhoIsUser {
    #[serde(rename = "LoginName", default)]
    login_name: String,
}

/// Why a peer was refused, for the router's log only. The caller sees the
/// same `Denied` for every one of these.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    #[error("no peer address on the connection")]
    NoPeer,
    #[error("not a tailnet address")]
    NotTailnet,
    #[error("tailscaled could not be asked: {0}")]
    Unreachable(String),
    #[error("tailscaled did not answer within {}s", WHOIS_TIMEOUT.as_secs())]
    TimedOut,
    #[error("tailscaled answered HTTP {0}")]
    Status(u16),
    #[error("tailscaled's answer could not be read: {0}")]
    Malformed(String),
    #[error("{0} is not in [otw] allow_users or allow_tags")]
    NotAllowed(String),
    #[error("{0} does not fold into a sealmesh host name")]
    Unnameable(String),
}

impl Refusal {
    /// Whether this is tailscaled's settled answer (worth caching) rather
    /// than a failure to get one (retried next connection).
    fn is_answer(&self) -> bool {
        matches!(self, Self::NotAllowed(_) | Self::Unnameable(_))
    }
}

/// The allowlist's decision on one whois reply. Pure, so every case is
/// tested without a socket.
fn decide(reply: WhoIsReply, allow: &Allowlist) -> Result<PeerIdentity, Refusal> {
    let node = reply
        .node
        .ok_or_else(|| Refusal::Malformed("no Node".to_owned()))?;
    let machine = machine_name(&node);

    if !node.tags.is_empty() {
        // A tagged node has no user (its UserProfile is Tailscale's
        // placeholder for tagged devices), so only its tags can admit it.
        let Some(tag) = node.tags.iter().find(|t| allow.tags.contains(t)) else {
            return Err(Refusal::NotAllowed(format!(
                "a node tagged {}",
                node.tags.join(",")
            )));
        };
        let host = host_from_login(tag).map_err(|_| Refusal::Unnameable(tag.clone()))?;
        return Ok(PeerIdentity { host, machine });
    }

    let login = reply
        .user
        .map(|u| u.login_name)
        .filter(|l| !l.trim().is_empty())
        .ok_or_else(|| Refusal::Malformed("no UserProfile.LoginName".to_owned()))?;
    if !allow.admits_user(&login) {
        return Err(Refusal::NotAllowed(login));
    }
    let host = host_from_login(&login).map_err(|_| Refusal::Unnameable(login.clone()))?;
    Ok(PeerIdentity { host, machine })
}

/// The node's name as a machine name: `ComputedName`, else the first label
/// of `Name`, folded the way an OS hostname is. `None` when neither folds,
/// which leaves the connection in an anonymous slot rather than refusing it.
fn machine_name(node: &WhoIsNode) -> Option<MachineName> {
    [node.computed_name.as_str(), node.name.as_str()]
        .into_iter()
        .filter(|n| !n.trim().is_empty())
        .find_map(MachineName::from_hostname)
}

type Verdict = Result<PeerIdentity, Refusal>;

#[derive(Default)]
struct Memory {
    answers: HashMap<IpAddr, (Instant, Verdict)>,
    logged: HashMap<IpAddr, Instant>,
}

impl Memory {
    fn prune(&mut self, now: Instant, ttl: Duration) {
        if self.answers.len() >= MAX_REMEMBERED {
            self.answers
                .retain(|_, (at, _)| now.saturating_duration_since(*at) < ttl);
            if self.answers.len() >= MAX_REMEMBERED {
                self.answers.clear();
            }
        }
        if self.logged.len() >= MAX_REMEMBERED {
            self.logged
                .retain(|_, at| now.saturating_duration_since(*at) < ttl);
            if self.logged.len() >= MAX_REMEMBERED {
                self.logged.clear();
            }
        }
    }
}

/// The directory an otw router runs with.
pub struct TailscaleDirectory {
    socket: PathBuf,
    allow: Allowlist,
    timeout: Duration,
    ttl: Duration,
    /// Whether a peer outside the tailnet ranges is still sent to whois.
    /// Only a test turns this on, so it can connect over loopback to a fake
    /// tailscaled; a running router refuses such a peer before asking.
    any_peer: bool,
    memory: Mutex<Memory>,
}

impl std::fmt::Debug for TailscaleDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TailscaleDirectory")
            .field("socket", &self.socket)
            .field("allow", &self.allow)
            .finish_non_exhaustive()
    }
}

impl TailscaleDirectory {
    /// A directory that asks the `tailscaled` LocalAPI at `socket` about each
    /// peer, and admits what `allow` lists.
    pub fn new(socket: PathBuf, allow: Allowlist) -> Self {
        Self {
            socket,
            allow,
            timeout: WHOIS_TIMEOUT,
            ttl: CACHE_TTL,
            any_peer: false,
            memory: Mutex::new(Memory::default()),
        }
    }

    pub fn socket(&self) -> &std::path::Path {
        &self.socket
    }

    pub fn allowlist(&self) -> &Allowlist {
        &self.allow
    }

    /// Sends loopback and other non-tailnet peers to whois too. Tests only:
    /// the fake tailscaled answers for 127.0.0.1.
    #[cfg(test)]
    fn for_tests(mut self, timeout: Duration) -> Self {
        self.any_peer = true;
        self.timeout = timeout;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Who `peer` is, by tailscaled's account, and whether it is admitted.
    pub async fn identify(&self, peer: Option<SocketAddr>) -> Result<PeerIdentity, Denied> {
        let verdict = self.verdict(peer).await;
        match verdict {
            Ok(identity) => Ok(identity),
            Err(refusal) => {
                self.log_once(peer, &refusal);
                Err(Denied::NotAuthorised)
            }
        }
    }

    async fn verdict(&self, peer: Option<SocketAddr>) -> Verdict {
        let Some(peer) = peer else {
            return Err(Refusal::NoPeer);
        };
        let ip = peer.ip().to_canonical();
        if !self.any_peer && !is_tailnet_ip(ip) {
            return Err(Refusal::NotTailnet);
        }

        let now = Instant::now();
        if let Some(cached) = self.cached(ip, now) {
            return cached;
        }

        let verdict = match tokio::time::timeout(self.timeout, whois(&self.socket, peer)).await {
            Err(_) => Err(Refusal::TimedOut),
            Ok(Err(refusal)) => Err(refusal),
            Ok(Ok(reply)) => decide(reply, &self.allow),
        };

        let settled = match &verdict {
            Ok(_) => true,
            Err(refusal) => refusal.is_answer(),
        };
        if settled && let Ok(mut memory) = self.memory.lock() {
            memory.prune(now, self.ttl);
            memory.answers.insert(ip, (now, verdict.clone()));
        }
        verdict
    }

    fn cached(&self, ip: IpAddr, now: Instant) -> Option<Verdict> {
        let memory = self.memory.lock().ok()?;
        let (at, verdict) = memory.answers.get(&ip)?;
        (now.saturating_duration_since(*at) < self.ttl).then(|| verdict.clone())
    }

    /// One log line per peer per [`CACHE_TTL`], so a daemon retrying against
    /// a refusal does not fill the log.
    fn log_once(&self, peer: Option<SocketAddr>, refusal: &Refusal) {
        let ip = peer.map(|p| p.ip().to_canonical());
        let now = Instant::now();
        if let (Some(ip), Ok(mut memory)) = (ip, self.memory.lock()) {
            let fresh = memory
                .logged
                .get(&ip)
                .is_some_and(|at| now.saturating_duration_since(*at) < self.ttl);
            if fresh {
                return;
            }
            memory.prune(now, self.ttl);
            memory.logged.insert(ip, now);
        }
        let who = ip.map_or_else(|| "a connection".to_owned(), |ip| ip.to_string());
        eprintln!("sealmesh-server: refused {who} (otw): {refusal}");
    }
}

impl Directory for TailscaleDirectory {
    /// A token proves nothing here: identity comes from the peer. Any path
    /// that asks by token alone is refused.
    async fn login(&self, _token: &SecretToken) -> Result<String, Denied> {
        Err(Denied::NotAuthorised)
    }

    async fn is_member(&self, _token: &SecretToken) -> Result<bool, Denied> {
        Ok(false)
    }

    /// The seam for per-repo access in otw mode. Only a connection that
    /// passed [`identify`](Self::identify) ever holds a token this is asked
    /// with, and an otw tailnet is one owner's machines, so everything is
    /// readable. A tailnet shared between people would decide here.
    async fn can_read(&self, _token: &SecretToken, _repo: &RepoSlug) -> RepoAccess {
        RepoAccess::Readable
    }

    /// Known without asking anyone, so no reader is charged a check budget
    /// for it.
    fn known_access(&self, _token: &SecretToken, _repo: &RepoSlug) -> Option<RepoAccess> {
        Some(RepoAccess::Readable)
    }

    // `recent_pulls` keeps the trait default: no pull requests are listed,
    // so archive-on-merge never fires.

    async fn peer_identity(
        &self,
        peer: Option<SocketAddr>,
    ) -> Option<Result<PeerIdentity, Denied>> {
        Some(self.identify(peer).await)
    }
}

/// `GET /localapi/v0/whois?addr=<ip:port>` over the LocalAPI Unix socket.
///
/// HTTP/1.0 on purpose: the server then never uses chunked encoding and ends
/// the body by closing, so reading to EOF (bounded) is the whole reply.
#[cfg(unix)]
async fn whois(socket: &std::path::Path, peer: SocketAddr) -> Result<WhoIsReply, Refusal> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let mut stream = tokio::net::UnixStream::connect(socket)
        .await
        .map_err(|e| Refusal::Unreachable(format!("{}: {e}", socket.display())))?;
    let request = format!(
        "GET /localapi/v0/whois?addr={} HTTP/1.0\r\nHost: {LOCALAPI_HOST}\r\n\
         Connection: close\r\n\r\n",
        query_escape(&peer.to_string())
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| Refusal::Unreachable(e.to_string()))?;

    let mut reply = Vec::new();
    let cap = u64::try_from(MAX_REPLY_BYTES)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut limited = (&mut stream).take(cap);
    limited
        .read_to_end(&mut reply)
        .await
        .map_err(|e| Refusal::Unreachable(e.to_string()))?;
    if reply.len() > MAX_REPLY_BYTES {
        return Err(Refusal::Malformed("reply too large".to_owned()));
    }
    parse_reply(&reply)
}

#[cfg(not(unix))]
async fn whois(_socket: &std::path::Path, _peer: SocketAddr) -> Result<WhoIsReply, Refusal> {
    Err(Refusal::Unreachable(
        "the LocalAPI Unix socket is not available on this platform".to_owned(),
    ))
}

/// Splits an HTTP/1.x reply into status and body, and decodes the body.
fn parse_reply(reply: &[u8]) -> Result<WhoIsReply, Refusal> {
    let split = reply
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| Refusal::Malformed("no end of headers".to_owned()))?;
    let (head, rest) = reply.split_at(split);
    let body = rest.get(4..).unwrap_or_default();
    let head = std::str::from_utf8(head)
        .map_err(|_| Refusal::Malformed("headers not UTF-8".to_owned()))?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let mut parts = status_line.split_whitespace();
    let version = parts.next().unwrap_or_default();
    if !version.starts_with("HTTP/1.") {
        return Err(Refusal::Malformed("not an HTTP/1 reply".to_owned()));
    }
    let status: u16 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Refusal::Malformed("no status code".to_owned()))?;
    if status != 200 {
        return Err(Refusal::Status(status));
    }
    for header in lines {
        if let Some((name, value)) = header.split_once(':')
            && name.trim().eq_ignore_ascii_case("transfer-encoding")
            && !value.trim().eq_ignore_ascii_case("identity")
        {
            return Err(Refusal::Malformed(
                "an encoded body to an HTTP/1.0 request".to_owned(),
            ));
        }
    }
    serde_json::from_slice(body).map_err(|e| Refusal::Malformed(format!("JSON: {e}")))
}

/// Percent-encodes the characters of an `ip:port` (or `[ip6]:port`) that a
/// query value must not carry raw, the way Go's `url.QueryEscape` does.
fn query_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 8);
    for c in value.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => out.push(c),
            other => {
                let mut buf = [0_u8; 4];
                for byte in other.encode_utf8(&mut buf).bytes() {
                    out.push_str(&format!("%{byte:02X}"));
                }
            }
        }
    }
    out
}

#[cfg(test)]
pub(crate) mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn allow(users: &[&str], tags: &[&str]) -> Allowlist {
        Allowlist::new(
            users.iter().map(|s| (*s).to_owned()).collect(),
            tags.iter().map(|s| (*s).to_owned()).collect(),
        )
        .unwrap()
    }

    pub(crate) fn user_reply(login: &str, node: &str) -> String {
        serde_json::json!({
            "Node": {
                "ID": 1,
                "Name": format!("{node}.tail1234.ts.net."),
                "ComputedName": node,
                "Addresses": ["100.101.102.103/32"],
                "CapMap": {}
            },
            "UserProfile": { "ID": 7, "LoginName": login, "DisplayName": "Someone" },
            "CapMap": null
        })
        .to_string()
    }

    pub(crate) fn tagged_reply(tags: &[&str], node: &str) -> String {
        serde_json::json!({
            "Node": {
                "Name": format!("{node}.tail1234.ts.net."),
                "ComputedName": node,
                "Tags": tags,
            },
            "UserProfile": { "LoginName": "tagged-devices" }
        })
        .to_string()
    }

    fn reply(json: &str) -> WhoIsReply {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn an_allowed_user_is_the_host_and_the_node_is_the_machine() {
        let identity = decide(
            reply(&user_reply("Alice@github", "work-laptop")),
            &allow(&["alice@github"], &[]),
        )
        .unwrap();
        assert_eq!(identity.host.as_str(), "alice-github");
        assert_eq!(identity.machine.unwrap().as_str(), "work-laptop");
    }

    #[test]
    fn the_machine_falls_back_to_the_first_label_of_the_fqdn() {
        let json = serde_json::json!({
            "Node": { "Name": "Desk-Top.tail1234.ts.net." },
            "UserProfile": { "LoginName": "alice@github" }
        })
        .to_string();
        let identity = decide(reply(&json), &allow(&["alice@github"], &[])).unwrap();
        assert_eq!(identity.machine.unwrap().as_str(), "desk-top");
    }

    #[test]
    fn a_tagged_node_is_named_by_its_first_allowed_tag_whatever_its_user() {
        let identity = decide(
            reply(&tagged_reply(&["tag:ci", "tag:sealmesh"], "build-box")),
            &allow(&["tagged-devices"], &["tag:sealmesh"]),
        )
        .unwrap();
        assert_eq!(identity.host.as_str(), "tag-sealmesh");
        assert_eq!(identity.machine.unwrap().as_str(), "build-box");

        // Its placeholder user never admits it: only a tag can.
        assert!(matches!(
            decide(
                reply(&tagged_reply(&["tag:ci"], "build-box")),
                &allow(&["tagged-devices"], &["tag:sealmesh"]),
            ),
            Err(Refusal::NotAllowed(_))
        ));
    }

    #[test]
    fn a_user_not_on_the_list_is_refused() {
        assert_eq!(
            decide(
                reply(&user_reply("mallory@github", "laptop")),
                &allow(&["alice@github"], &["tag:sealmesh"]),
            ),
            Err(Refusal::NotAllowed("mallory@github".to_owned()))
        );
    }

    #[test]
    fn a_reply_without_a_node_or_a_login_is_malformed() {
        for json in [
            r#"{}"#,
            r#"{"UserProfile":{"LoginName":"alice@github"}}"#,
            r#"{"Node":{"Name":"x.ts.net."}}"#,
            r#"{"Node":{"Name":"x.ts.net."},"UserProfile":{"LoginName":" "}}"#,
        ] {
            assert!(
                matches!(
                    decide(reply(json), &allow(&["alice@github"], &[])),
                    Err(Refusal::Malformed(_))
                ),
                "{json}"
            );
        }
    }

    #[test]
    fn an_empty_or_bad_allowlist_is_refused_at_startup() {
        assert!(Allowlist::new(vec![], vec![]).is_err());
        assert!(Allowlist::new(vec![" ".to_owned()], vec![]).is_err());
        assert!(Allowlist::new(vec![], vec!["sealmesh".to_owned()]).is_err());
        assert!(Allowlist::new(vec![], vec!["tag:".to_owned()]).is_err());
        assert!(Allowlist::new(vec!["a".repeat(60)], vec![]).is_err());
        assert!(Allowlist::new(vec![], vec!["tag:sealmesh".to_owned()]).is_ok());
    }

    #[test]
    fn replies_are_parsed_by_status_and_body() {
        let ok = format!(
            "HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n{}",
            user_reply("alice@github", "laptop")
        );
        assert!(parse_reply(ok.as_bytes()).is_ok());
        assert_eq!(
            parse_reply(b"HTTP/1.0 404 Not Found\r\n\r\npeer not found").unwrap_err(),
            Refusal::Status(404)
        );
        for bad in [
            &b"HTTP/1.0 200 OK\r\n\r\nnot json"[..],
            b"HTTP/1.0 200 OK\r\nno end of headers",
            b"SSH-2.0-OpenSSH\r\n\r\n{}",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n",
        ] {
            assert!(
                matches!(parse_reply(bad), Err(Refusal::Malformed(_))),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn the_peer_address_is_escaped_like_go_does() {
        assert_eq!(query_escape("100.1.2.3:41641"), "100.1.2.3%3A41641");
        assert_eq!(
            query_escape("[fd7a:115c:a1e0::1]:80"),
            "%5Bfd7a%3A115c%3Aa1e0%3A%3A1%5D%3A80"
        );
    }

    #[tokio::test]
    async fn a_peer_off_the_tailnet_or_without_an_address_is_refused_before_asking() {
        // The socket does not exist: were whois asked, the refusal would be
        // `Unreachable`, not these.
        let directory = TailscaleDirectory::new(
            PathBuf::from("/nonexistent/tailscaled.sock"),
            allow(&["alice@github"], &[]),
        );
        assert_eq!(directory.verdict(None).await, Err(Refusal::NoPeer));
        for peer in ["127.0.0.1:5000", "192.168.1.4:5000", "[::1]:5000"] {
            assert_eq!(
                directory.verdict(Some(peer.parse().unwrap())).await,
                Err(Refusal::NotTailnet),
                "{peer}"
            );
        }
        assert!(matches!(
            directory
                .verdict(Some("100.64.0.9:5000".parse().unwrap()))
                .await,
            Err(Refusal::Unreachable(_))
        ));
        assert_eq!(
            directory
                .identify(Some("100.64.0.9:5000".parse().unwrap()))
                .await,
            Err(Denied::NotAuthorised)
        );
    }

    /// A stand-in for tailscaled's LocalAPI on a temp Unix socket. Each
    /// connection reads one request, records its first line, and answers with
    /// whatever `respond` returns (or stalls, for `None`).
    #[cfg(unix)]
    pub(crate) struct FakeTailscaled {
        pub dir: PathBuf,
        pub socket: PathBuf,
        pub requests: std::sync::Arc<Mutex<Vec<String>>>,
        task: tokio::task::JoinHandle<()>,
    }

    #[cfg(unix)]
    impl FakeTailscaled {
        pub(crate) fn start(
            respond: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
        ) -> Self {
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Short: a Unix socket path is capped near 104 bytes on macOS.
            let dir = std::env::temp_dir().join(format!("smts-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let socket = dir.join("ts.sock");
            let _ = std::fs::remove_file(&socket);
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            let requests = std::sync::Arc::new(Mutex::new(Vec::new()));
            let seen = std::sync::Arc::clone(&requests);
            let respond = std::sync::Arc::new(respond);
            let task = tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        return;
                    };
                    let seen = std::sync::Arc::clone(&seen);
                    let respond = std::sync::Arc::clone(&respond);
                    tokio::spawn(async move {
                        let mut buf = Vec::new();
                        let mut chunk = [0_u8; 1024];
                        while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                            match stream.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(chunk.get(..n).unwrap()),
                            }
                        }
                        let text = String::from_utf8_lossy(&buf).to_string();
                        let first = text.lines().next().unwrap_or_default().to_owned();
                        seen.lock().unwrap().push(text);
                        match respond(&first) {
                            Some(answer) => {
                                let _ = stream.write_all(answer.as_bytes()).await;
                                let _ = stream.shutdown().await;
                            }
                            None => tokio::time::sleep(Duration::from_secs(30)).await,
                        }
                    });
                }
            });
            Self {
                dir,
                socket,
                requests,
                task,
            }
        }

        pub(crate) fn ok(json: String) -> impl Fn(&str) -> Option<String> + Send + Sync {
            move |_| {
                Some(format!(
                    "HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n{json}"
                ))
            }
        }

        pub(crate) fn asked(&self) -> usize {
            self.requests.lock().unwrap().len()
        }
    }

    #[cfg(unix)]
    impl Drop for FakeTailscaled {
        fn drop(&mut self) {
            self.task.abort();
            std::fs::remove_dir_all(&self.dir).ok();
        }
    }

    #[cfg(unix)]
    pub(crate) fn directory_for(fake: &FakeTailscaled, allow: Allowlist) -> TailscaleDirectory {
        TailscaleDirectory::new(fake.socket.clone(), allow).for_tests(Duration::from_millis(300))
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn whois_asks_localapi_for_the_peer_and_caches_the_answer() {
        let fake = FakeTailscaled::start(FakeTailscaled::ok(user_reply("alice@github", "laptop")));
        let directory = directory_for(&fake, allow(&["alice@github"], &[]));
        let peer: SocketAddr = "100.100.1.2:41641".parse().unwrap();

        let identity = directory.identify(Some(peer)).await.unwrap();
        assert_eq!(identity.host.as_str(), "alice-github");
        assert_eq!(identity.machine.unwrap().as_str(), "laptop");

        let request = fake.requests.lock().unwrap().first().cloned().unwrap();
        assert!(
            request.starts_with("GET /localapi/v0/whois?addr=100.100.1.2%3A41641 HTTP/1.0\r\n"),
            "{request}"
        );
        assert!(
            request.contains("\r\nHost: local-tailscaled.sock\r\n"),
            "{request}"
        );

        // Same address, another port: answered from the cache.
        directory
            .identify(Some("100.100.1.2:50000".parse().unwrap()))
            .await
            .unwrap();
        assert_eq!(fake.asked(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_cached_answer_expires() {
        let fake = FakeTailscaled::start(FakeTailscaled::ok(user_reply("alice@github", "laptop")));
        let directory =
            directory_for(&fake, allow(&["alice@github"], &[])).with_ttl(Duration::from_millis(50));
        let peer: SocketAddr = "100.100.1.2:41641".parse().unwrap();
        directory.identify(Some(peer)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        directory.identify(Some(peer)).await.unwrap();
        assert_eq!(fake.asked(), 2);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn every_failure_to_get_an_answer_is_refused_and_not_cached() {
        for (what, respond) in [
            (
                "500",
                Box::new(|_: &str| {
                    Some("HTTP/1.0 500 Internal Server Error\r\n\r\nboom".to_owned())
                }) as Box<dyn Fn(&str) -> Option<String> + Send + Sync>,
            ),
            ("timeout", Box::new(|_: &str| None)),
            (
                "malformed",
                Box::new(|_: &str| Some("HTTP/1.0 200 OK\r\n\r\n{\"Node\":".to_owned())),
            ),
        ] {
            let fake = FakeTailscaled::start(respond);
            let directory = directory_for(&fake, allow(&["alice@github"], &[]));
            let peer: SocketAddr = "100.100.1.2:41641".parse().unwrap();
            assert_eq!(
                directory.identify(Some(peer)).await,
                Err(Denied::NotAuthorised),
                "{what} failed open"
            );
            assert_eq!(
                directory.identify(Some(peer)).await,
                Err(Denied::NotAuthorised),
                "{what}"
            );
            assert_eq!(fake.asked(), 2, "{what}: a failure was cached");
        }
    }

    #[tokio::test]
    async fn tailscaled_down_is_refused() {
        let directory = TailscaleDirectory::new(
            std::env::temp_dir().join("smts-absent/ts.sock"),
            allow(&["alice@github"], &[]),
        );
        assert_eq!(
            directory
                .identify(Some("100.100.1.2:41641".parse().unwrap()))
                .await,
            Err(Denied::NotAuthorised)
        );
    }

    #[tokio::test]
    async fn a_token_alone_proves_nothing() {
        let directory =
            TailscaleDirectory::new(PathBuf::from("/nonexistent"), allow(&["alice@github"], &[]));
        let token = SecretToken::new("alice@github".to_owned());
        assert_eq!(directory.login(&token).await, Err(Denied::NotAuthorised));
        assert_eq!(directory.is_member(&token).await, Ok(false));
        assert_eq!(
            crate::auth::authenticate(&directory, &token).await,
            Err(Denied::NotAuthorised)
        );
    }
}
