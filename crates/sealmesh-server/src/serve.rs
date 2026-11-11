//! The embeddable router entry: serve an [`App`] on a listener.
//!
//! `sealmesh-server`'s own `main` builds a cloud app (GitHub, the access cache,
//! rooms from its environment) and serves it on TCP through [`run`]. A std
//! daemon builds its app over a `LocalOwner` and serves it on a Unix socket
//! through the same call, so both transports reach one router: the same
//! `/v1/connect` handshake, frames and limits.

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};

use crate::{
    auth::Directory,
    http::{App, router},
};

/// Where a router accepts connections.
pub enum Listener {
    Tcp(tokio::net::TcpListener),
    #[cfg(unix)]
    Unix(tokio::net::UnixListener),
}

impl From<tokio::net::TcpListener> for Listener {
    fn from(listener: tokio::net::TcpListener) -> Self {
        Self::Tcp(listener)
    }
}

#[cfg(unix)]
impl From<tokio::net::UnixListener> for Listener {
    fn from(listener: tokio::net::UnixListener) -> Self {
        Self::Unix(listener)
    }
}

/// Serves `app` on `listener` until the listener fails. Never returns `Ok`
/// under normal operation.
///
/// A TCP connection records its peer address for the handshake, so a
/// directory that identifies by peer (otw: Tailscale whois) can ask about it.
/// A directory that identifies by token never looks at it.
pub async fn run<D: Directory + 'static>(app: App<D>, listener: Listener) -> Result<()> {
    let router = router(Arc::new(app));
    match listener {
        Listener::Tcp(listener) => axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .context("serving"),
        #[cfg(unix)]
        Listener::Unix(listener) => axum::serve(listener, router).await.context("serving"),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::time::Duration;

    use futures_util::{SinkExt as _, StreamExt as _};
    use sealmesh_proto::{ClientFrame, Role, SecretToken, ServerFrame};
    use tokio_tungstenite::tungstenite::Message;

    use super::*;
    use crate::{
        history::Fake, local_owner::LocalOwner, mode_directory::ModeDirectory, projects::ProjectMap,
    };

    fn app(secret: &str) -> App<ModeDirectory> {
        let owner = LocalOwner::new("me", SecretToken::new(secret.to_owned())).unwrap();
        App::new(
            ModeDirectory::from(owner),
            ProjectMap::empty(),
            Arc::new(Fake::new()),
        )
    }

    async fn handshake<S>(ws: tokio_tungstenite::WebSocketStream<S>, token: &str) -> ServerFrame
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        handshake_as(ws, token, Role::Reader).await.0
    }

    async fn handshake_as<S>(
        mut ws: tokio_tungstenite::WebSocketStream<S>,
        token: &str,
        role: Role,
    ) -> (ServerFrame, tokio_tungstenite::WebSocketStream<S>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let hello = ClientFrame::Hello {
            role,
            token: SecretToken::new(token.to_owned()),
        };
        ws.send(Message::Text(serde_json::to_string(&hello).unwrap().into()))
            .await
            .unwrap();
        loop {
            let next = tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .expect("no answer to Hello within 5s");
            match next {
                Some(Ok(Message::Text(text))) => {
                    return (serde_json::from_str(text.as_str()).unwrap(), ws);
                }
                Some(Ok(_)) => continue,
                other => panic!("socket ended before an answer: {other:?}"),
            }
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn serves_over_a_unix_socket_with_the_local_secret() {
        let dir = std::env::temp_dir().join(format!("sealmesh-serve-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("router.sock");
        let _ = std::fs::remove_file(&path);
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(run(app("local-secret"), Listener::from(listener)));

        for (token, welcomed) in [("local-secret", true), ("wrong", false)] {
            let stream = tokio::net::UnixStream::connect(&path).await.unwrap();
            let (ws, _) = tokio_tungstenite::client_async("ws://localhost/v1/connect", stream)
                .await
                .unwrap();
            match handshake(ws, token).await {
                ServerFrame::Welcome { host } if welcomed => assert_eq!(host.as_str(), "me"),
                ServerFrame::Denied {} if !welcomed => {}
                other => panic!("{token}: unexpected {other:?}"),
            }
        }

        server.abort();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn serves_over_tcp_as_before() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(run(app("tcp-secret"), Listener::from(listener)));

        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/v1/connect"))
            .await
            .unwrap();
        assert!(matches!(
            handshake(ws, "tcp-secret").await,
            ServerFrame::Welcome { .. }
        ));

        server.abort();
    }

    // otw: identity from a fake tailscaled's whois, never from the token.

    use std::net::SocketAddr;

    use crate::{
        registry::Target,
        tailscale::{
            Allowlist, TailscaleDirectory,
            tests::{FakeTailscaled, directory_for, tagged_reply, user_reply},
        },
    };

    fn otw_app(directory: TailscaleDirectory) -> App<ModeDirectory> {
        App::new(
            ModeDirectory::from(directory),
            ProjectMap::empty(),
            Arc::new(Fake::new()),
        )
    }

    fn allow(users: &[&str], tags: &[&str]) -> Allowlist {
        Allowlist::new(
            users.iter().map(|s| (*s).to_owned()).collect(),
            tags.iter().map(|s| (*s).to_owned()).collect(),
        )
        .unwrap()
    }

    /// Serves `app` through [`run`] on loopback, and returns its address.
    async fn serve_otw(
        directory: TailscaleDirectory,
    ) -> (SocketAddr, tokio::task::JoinHandle<Result<()>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(run(otw_app(directory), Listener::from(listener)));
        (addr, server)
    }

    async fn otw_hello(addr: SocketAddr) -> ServerFrame {
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/v1/connect"))
            .await
            .unwrap();
        // The token is empty, as an otw daemon sends it.
        handshake(ws, "").await
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn otw_welcomes_an_allowed_user_as_its_login() {
        let fake = FakeTailscaled::start(FakeTailscaled::ok(user_reply("Alice@github", "laptop")));
        let (addr, server) = serve_otw(directory_for(&fake, allow(&["alice@github"], &[]))).await;
        match otw_hello(addr).await {
            ServerFrame::Welcome { host } => assert_eq!(host.as_str(), "alice-github"),
            other => panic!("unexpected {other:?}"),
        }
        // The router asked about the connection's own address.
        let request = fake.requests.lock().unwrap().first().cloned().unwrap();
        assert!(request.contains("addr=127.0.0.1%3A"), "{request}");
        server.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn otw_welcomes_an_allowed_tag() {
        let fake = FakeTailscaled::start(FakeTailscaled::ok(tagged_reply(&["tag:sealmesh"], "ci")));
        let (addr, server) = serve_otw(directory_for(&fake, allow(&[], &["tag:sealmesh"]))).await;
        match otw_hello(addr).await {
            ServerFrame::Welcome { host } => assert_eq!(host.as_str(), "tag-sealmesh"),
            other => panic!("unexpected {other:?}"),
        }
        server.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn otw_denies_whoever_is_not_allowed_and_whatever_cannot_be_asked() {
        let cases: Vec<(&str, FakeTailscaled)> = vec![
            (
                "not allowed",
                FakeTailscaled::start(FakeTailscaled::ok(user_reply("mallory@github", "box"))),
            ),
            (
                "500",
                FakeTailscaled::start(|_: &str| {
                    Some("HTTP/1.0 500 Internal Server Error\r\n\r\nno".to_owned())
                }),
            ),
            ("timeout", FakeTailscaled::start(|_: &str| None)),
            (
                "malformed",
                FakeTailscaled::start(|_: &str| Some("HTTP/1.0 200 OK\r\n\r\n[]".to_owned())),
            ),
        ];
        for (what, fake) in &cases {
            let (addr, server) = serve_otw(directory_for(
                fake,
                allow(&["alice@github"], &["tag:sealmesh"]),
            ))
            .await;
            assert!(
                matches!(otw_hello(addr).await, ServerFrame::Denied {}),
                "{what} was not denied"
            );
            server.abort();
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn otw_with_tailscaled_down_denies() {
        let directory = TailscaleDirectory::new(
            std::env::temp_dir().join("smts-gone/ts.sock"),
            allow(&["alice@github"], &[]),
        );
        // Not `for_tests`: a loopback peer is refused before whois is asked,
        // which is the same Denied a running router gives it.
        let (addr, server) = serve_otw(directory).await;
        assert!(matches!(otw_hello(addr).await, ServerFrame::Denied {}));
        server.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn otw_keeps_two_machines_of_one_person_attached_under_their_node_names() {
        let asked = std::sync::atomic::AtomicUsize::new(0);
        let fake = FakeTailscaled::start(move |_: &str| {
            let node = match asked.fetch_add(1, std::sync::atomic::Ordering::Relaxed) {
                0 => "laptop",
                _ => "desktop",
            };
            Some(format!(
                "HTTP/1.0 200 OK\r\n\r\n{}",
                user_reply("alice@github", node)
            ))
        });
        // No cache, so each connection from 127.0.0.1 is asked about afresh.
        let directory =
            directory_for(&fake, allow(&["alice@github"], &[])).with_ttl(Duration::ZERO);
        let app = Arc::new(otw_app(directory));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service = crate::http::router(Arc::clone(&app))
            .into_make_service_with_connect_info::<SocketAddr>();
        let server = tokio::spawn(async move { axum::serve(listener, service).await });

        let mut daemons = Vec::new();
        for _ in 0..2 {
            let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/v1/connect"))
                .await
                .unwrap();
            let (frame, mut ws) = handshake_as(ws, "", Role::Daemon).await;
            assert!(matches!(frame, ServerFrame::Welcome { .. }), "{frame:?}");
            // The daemon's own claim is ignored: the router named it already.
            let claim = ClientFrame::Machine {
                name: sealmesh_proto::MachineName::try_from("spoofed").unwrap(),
            };
            ws.send(Message::Text(serde_json::to_string(&claim).unwrap().into()))
                .await
                .unwrap();
            daemons.push(ws);
        }

        let mut live = Vec::new();
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            live = app
                .registry
                .targets()
                .into_iter()
                .filter_map(|target| match target {
                    Target::Live(_, who) => Some((
                        who.host.as_str().to_owned(),
                        who.machine.map(|m| m.as_str().to_owned()),
                    )),
                    Target::Gone(_) => None,
                })
                .collect::<Vec<_>>();
            if live.len() == 2 {
                break;
            }
        }
        live.sort();
        assert_eq!(
            live,
            [
                ("alice-github".to_owned(), Some("desktop".to_owned())),
                ("alice-github".to_owned(), Some("laptop".to_owned())),
            ]
        );
        drop(daemons);
        server.abort();
    }
}
