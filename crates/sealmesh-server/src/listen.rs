use std::net::SocketAddr;

pub const TLS_TERMINATOR_VAR: &str = "SEALMESH_TLS_TERMINATED_BY";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "refusing to serve plaintext on {addr}, which is reachable from off this machine. \
     The handshake carries a GitHub token, so a plaintext hop leaks a working credential \
     for the organisation. Either put TLS in front of this process and set \
     {TLS_TERMINATOR_VAR} to name what does it (e.g. `{TLS_TERMINATOR_VAR}=alb`), or bind \
     a loopback address for local development."
)]
pub struct PlaintextRefused {
    pub addr: SocketAddr,
}

pub fn permit_plaintext(
    addr: SocketAddr,
    tls_terminator: Option<&str>,
) -> Result<(), PlaintextRefused> {
    if addr.ip().is_loopback() {
        return Ok(());
    }
    match tls_terminator {
        Some(name) if !name.trim().is_empty() => Ok(()),
        _ => Err(PlaintextRefused { addr }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "refusing to serve otw mode on {addr}, which is not a tailnet address. An otw router \
     must bind this machine's Tailscale address (in 100.64.0.0/10 or fd7a:115c:a1e0::/48; \
     `tailscale ip -4` prints it), so that only the tailnet can reach it and every peer \
     can be identified with Tailscale whois. 0.0.0.0, :: and LAN or public addresses are \
     refused."
)]
pub struct OffTailnetRefused {
    pub addr: SocketAddr,
}

/// The otw-mode version of [`permit_plaintext`]: plaintext is allowed on a
/// tailnet address and nowhere else, with no TLS terminator to declare.
///
/// WireGuard already encrypts and authenticates every hop between tailnet
/// nodes, and the handshake carries no credential in otw mode (identity comes
/// from whois), so a plain WebSocket on the tailnet address leaks nothing. A
/// wildcard bind would also be reachable off the tailnet, by peers whois could
/// not vouch for, so it is refused. Loopback is refused too unless
/// `allow_loopback`, which only a test sets.
pub fn permit_tailnet_bind(
    addr: SocketAddr,
    allow_loopback: bool,
) -> Result<(), OffTailnetRefused> {
    let ip = addr.ip().to_canonical();
    if sealmesh_proto::tailnet::is_tailnet_ip(ip) || (allow_loopback && ip.is_loopback()) {
        Ok(())
    } else {
        Err(OffTailnetRefused { addr })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn an_off_machine_address_without_a_declared_terminator_is_refused() {
        for reachable in ["0.0.0.0:8787", "10.0.1.7:8787", "[::]:8787"] {
            assert_eq!(
                permit_plaintext(addr(reachable), None),
                Err(PlaintextRefused {
                    addr: addr(reachable)
                }),
                "started plaintext on {reachable}"
            );
        }
    }

    #[test]
    fn an_empty_or_blank_declaration_is_not_a_declaration() {
        for blank in ["", " ", "\t\n"] {
            assert!(
                permit_plaintext(addr("0.0.0.0:8787"), Some(blank)).is_err(),
                "accepted {blank:?} as a TLS terminator"
            );
        }
    }

    #[test]
    fn a_named_terminator_permits_the_deployed_arrangement() {
        assert_eq!(permit_plaintext(addr("0.0.0.0:8787"), Some("alb")), Ok(()));
    }

    #[test]
    fn loopback_needs_no_declaration() {
        for local in ["127.0.0.1:8787", "[::1]:8787"] {
            assert_eq!(permit_plaintext(addr(local), None), Ok(()), "{local}");
        }
    }

    #[test]
    fn otw_binds_only_a_tailnet_address() {
        for tailnet in [
            "100.64.0.1:8787",
            "100.101.102.103:8787",
            "[fd7a:115c:a1e0::1]:8787",
        ] {
            assert_eq!(
                permit_tailnet_bind(addr(tailnet), false),
                Ok(()),
                "{tailnet}"
            );
        }
        for off in [
            "0.0.0.0:8787",
            "[::]:8787",
            "203.0.113.7:8787",
            "8.8.8.8:8787",
            "192.168.1.20:8787",
            "100.128.0.1:8787",
            "127.0.0.1:8787",
            "[::1]:8787",
        ] {
            assert_eq!(
                permit_tailnet_bind(addr(off), false),
                Err(OffTailnetRefused { addr: addr(off) }),
                "bound otw on {off}"
            );
        }
    }

    #[test]
    fn otw_loopback_is_only_for_a_test_that_asks() {
        assert!(permit_tailnet_bind(addr("127.0.0.1:8787"), true).is_ok());
        assert!(permit_tailnet_bind(addr("0.0.0.0:8787"), true).is_err());
        let message = permit_tailnet_bind(addr("0.0.0.0:8787"), false)
            .unwrap_err()
            .to_string();
        assert!(message.contains("tailscale ip"), "{message}");
    }

    #[test]
    fn the_refusal_names_the_variable_and_the_way_out() {
        let refused = permit_plaintext(addr("0.0.0.0:8787"), None).unwrap_err();
        let message = refused.to_string();
        assert!(message.contains(TLS_TERMINATOR_VAR), "{message}");
        assert!(message.contains("loopback"), "{message}");
    }
}
