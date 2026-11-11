//! Tailscale address ranges, shared by the otw router (which binds only to
//! one) and the daemon (whose `[server] url` must name one).
//!
//! Tailscale hands every node an IPv4 address from the CGNAT range
//! `100.64.0.0/10` and an IPv6 address from its own ULA prefix
//! `fd7a:115c:a1e0::/48`. An address outside both is not on a tailnet, so a
//! router bound to it would be reachable by whatever else routes there, and a
//! daemon pointed at it would not be talking over WireGuard.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Whether `ip` is in a range Tailscale assigns node addresses from.
pub fn is_tailnet_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_tailnet_v4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => is_tailnet_v4(v4),
            None => is_tailnet_v6(v6),
        },
    }
}

/// `100.64.0.0/10`: the first octet is 100 and the top two bits of the second
/// are `01`.
fn is_tailnet_v4(ip: Ipv4Addr) -> bool {
    let [a, b, _, _] = ip.octets();
    a == 100 && (b & 0b1100_0000) == 0b0100_0000
}

/// `fd7a:115c:a1e0::/48`: the first three 16-bit segments.
fn is_tailnet_v6(ip: Ipv6Addr) -> bool {
    let [a, b, c, ..] = ip.segments();
    a == 0xfd7a && b == 0x115c && c == 0xa1e0
}

/// The `tailscaled` LocalAPI socket of the open-source daemon on this
/// platform (Tailscale's `paths.DefaultTailscaledSocket`). The macOS App
/// Store and standalone apps serve LocalAPI elsewhere, not on this socket.
pub fn default_tailscaled_socket() -> std::path::PathBuf {
    if cfg!(target_os = "macos") {
        std::path::PathBuf::from("/var/run/tailscaled.socket")
    } else {
        std::path::PathBuf::from("/var/run/tailscale/tailscaled.sock")
    }
}

/// Whether `host` is a MagicDNS name: a name under `.ts.net`, or a bare host
/// name with no dot, which MagicDNS resolves inside the tailnet.
pub fn is_magic_dns_name(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() || host == "localhost" {
        return false;
    }
    host.ends_with(".ts.net") || !host.contains('.')
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn tailnet_ranges_are_recognised_at_their_edges() {
        for inside in [
            "100.64.0.0",
            "100.100.100.100",
            "100.127.255.255",
            "fd7a:115c:a1e0::1",
            "fd7a:115c:a1e0:ffff:ffff:ffff:ffff:ffff",
            "::ffff:100.101.102.103",
        ] {
            assert!(is_tailnet_ip(ip(inside)), "{inside}");
        }
        for outside in [
            "100.63.255.255",
            "100.128.0.0",
            "0.0.0.0",
            "127.0.0.1",
            "10.0.0.1",
            "192.168.1.10",
            "8.8.8.8",
            "::",
            "::1",
            "fd7a:115c:a1e1::1",
            "2001:db8::1",
        ] {
            assert!(!is_tailnet_ip(ip(outside)), "{outside}");
        }
    }

    #[test]
    fn magic_dns_names_are_ts_net_or_bare() {
        for name in [
            "router",
            "router.tail1234.ts.net",
            "Router.TAIL1234.ts.net.",
        ] {
            assert!(is_magic_dns_name(name), "{name}");
        }
        for name in ["", "localhost", "router.example.com", "ts.net.example.com"] {
            assert!(!is_magic_dns_name(name), "{name}");
        }
    }
}
