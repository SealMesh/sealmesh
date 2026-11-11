//! The embeddable router entry: serve an [`App`] on a listener.
//!
//! `sealmesh-server`'s own `main` builds a cloud app (GitHub, the access cache,
//! rooms from its environment) and serves it on TCP through [`run`]. A std
//! daemon builds its app over a `LocalOwner` and serves it on a Unix socket
//! through the same call, so both transports reach one router: the same
//! `/v1/connect` handshake, frames and limits.

use std::sync::Arc;

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
pub async fn run<D: Directory + 'static>(app: App<D>, listener: Listener) -> Result<()> {
    let router = router(Arc::new(app));
    match listener {
        Listener::Tcp(listener) => axum::serve(listener, router).await.context("serving"),
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

    async fn handshake<S>(mut ws: tokio_tungstenite::WebSocketStream<S>, token: &str) -> ServerFrame
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let hello = ClientFrame::Hello {
            role: Role::Reader,
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
                    return serde_json::from_str(text.as_str()).unwrap();
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
}
