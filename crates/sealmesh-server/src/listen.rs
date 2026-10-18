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
    fn the_refusal_names_the_variable_and_the_way_out() {
        let refused = permit_plaintext(addr("0.0.0.0:8787"), None).unwrap_err();
        let message = refused.to_string();
        assert!(message.contains(TLS_TERMINATOR_VAR), "{message}");
        assert!(message.contains("loopback"), "{message}");
    }
}
