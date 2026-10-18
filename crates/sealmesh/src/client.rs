use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use sealmesh_chat_wire::RoomId;
use sealmesh_proto::{ClientFrame, MAX_FRAME_BYTES, ProjectEntry, Role, ServerFrame, decode};
use sealmesh_wire::HostId;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

use crate::{config::ServerConfig, github_auth};

pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// How long to wait for a `ServerFrame::Projects` reply before giving up and
/// rendering ungrouped. A router built before this shipped does not
/// recognise `ClientFrame::Projects` at all, so it never answers — this
/// bounds that wait rather than hanging `who` forever on an old router.
const PROJECTS_TIMEOUT: Duration = Duration::from_secs(3);

pub struct Connected {
    pub socket: Socket,
    pub host: HostId,
}

pub async fn connect(config: &ServerConfig, role: Role) -> Result<Connected> {
    let url = config.connect_url()?;
    let token = github_auth::token()?;

    let limits = WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME_BYTES))
        .max_frame_size(Some(MAX_FRAME_BYTES));
    let (mut socket, _response) = connect_async_with_config(&url, Some(limits), false)
        .await
        .with_context(|| format!("connecting to {url}"))?;

    send(&mut socket, &ClientFrame::Hello { role, token }).await?;

    match recv_frame(&mut socket).await? {
        ServerFrame::Welcome { host } => Ok(Connected { socket, host }),
        ServerFrame::Denied {} => bail!(
            "the router refused this login. Check `gh auth status`, and that you are an \
             active member of the GitHub organisation this router serves"
        ),
        _ => bail!("router answered the handshake with something else"),
    }
}

/// Asks the router for its repo→project mapping. `None` means no reply
/// arrived within `PROJECTS_TIMEOUT` — indistinguishable, from here, between
/// an old router that does not know this frame and a new one that is simply
/// slow — and the caller falls back to ungrouped rendering either way. A
/// real, empty mapping (`Some(vec![])`) is a different situation on the wire
/// (see the proto crate's tests) but renders the same way.
pub async fn request_projects(socket: &mut Socket) -> Result<Option<Vec<ProjectEntry>>> {
    send(socket, &ClientFrame::Projects {}).await?;
    match within(PROJECTS_TIMEOUT, wait_for_projects(socket)).await {
        Some(result) => result.map(Some),
        None => Ok(None),
    }
}

async fn wait_for_projects(socket: &mut Socket) -> Result<Vec<ProjectEntry>> {
    loop {
        match recv(socket).await? {
            Incoming::Frame(ServerFrame::Projects { projects }) => return Ok(projects),
            Incoming::Frame(_)
            | Incoming::Noise
            | Incoming::Undecodable
            | Incoming::UnknownRefusal(_) => continue,
        }
    }
}

async fn within<T>(limit: Duration, step: impl Future<Output = T>) -> Option<T> {
    tokio::time::timeout(limit, step).await.ok()
}

pub async fn send(socket: &mut Socket, frame: &ClientFrame) -> Result<()> {
    use futures_util::SinkExt as _;

    let body = serde_json::to_string(frame).context("encoding a frame")?;
    socket
        .send(Message::Text(body.into()))
        .await
        .context("sending a frame")
}

pub async fn ping(socket: &mut Socket) -> Result<()> {
    use futures_util::SinkExt as _;

    socket
        .send(Message::Ping(Vec::new().into()))
        .await
        .context("sending a heartbeat")
}

#[derive(Debug)]
pub enum Incoming {
    Frame(ServerFrame),
    Noise,
    /// A text frame that did not decode. The router is a separate program and
    /// may be a version ahead, so one unreadable frame must not cost the caller
    /// every other frame in the stream — a reader would lose every machine's
    /// rows because one machine's answer was too new.
    Undecodable,
    /// A `PostRefused` or `BackfillRefused` whose category this build does not
    /// know. Still a refusal of something this daemon asked for, so it is
    /// kept rather than skipped as undecodable — see [`UnknownRefusal`].
    UnknownRefusal(UnknownRefusal),
}

/// A refusal from a router newer than this build, with its category dropped.
///
/// Laptops upgrade by hand, so the router is routinely a version ahead, and
/// `PostRefusal` and `BackfillRefusal` are closed enums that a new category
/// makes undecodable. Skipping that frame would lose the refusal silently: a
/// post would look accepted and a history read would hang until its deadline.
/// So this build's decode path — and only this one — recognises the two
/// refusal shapes with any string category, and hands them on as this case,
/// which the daemon records and reports as `unknown`. It is a deliberate,
/// client-side exception to rule 43; the router's own strict decoding and the
/// proto crate's closed enums are untouched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnknownRefusal {
    Post { room: RoomId },
    Backfill { room: RoomId },
}

/// The two refusal frames, with any string where the category goes. Every
/// other field is as strict as the real frame, and `deny_unknown_fields`
/// still applies, so this admits exactly one new thing: a category word.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Tolerant {
    PostRefused {
        room: RoomId,
        #[serde(rename = "category")]
        _category: Discarded,
    },
    BackfillRefused {
        room: RoomId,
        #[serde(rename = "category")]
        _category: Discarded,
    },
}

/// A string, read and thrown away. It came from a peer, so it is not kept,
/// logged or echoed (rule 14); the frame cap already bounds its length.
struct Discarded;

impl<'de> Deserialize<'de> for Discarded {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = Discarded;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a category word")
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Discarded, E> {
                Ok(Discarded)
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}

pub async fn recv(socket: &mut Socket) -> Result<Incoming> {
    use futures_util::StreamExt as _;

    let message = socket
        .next()
        .await
        .ok_or_else(|| anyhow!("the router closed the connection"))?
        .context("reading from the router")?;

    match message {
        Message::Text(text) => Ok(classify(text.as_bytes())),
        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => Ok(Incoming::Noise),
        Message::Binary(_) | Message::Close(_) => bail!("the router closed the connection"),
    }
}

fn classify(text: &[u8]) -> Incoming {
    if let Ok(frame) = decode(text) {
        return Incoming::Frame(frame);
    }
    match decode::<Tolerant>(text) {
        Ok(Tolerant::PostRefused { room, .. }) => {
            Incoming::UnknownRefusal(UnknownRefusal::Post { room })
        }
        Ok(Tolerant::BackfillRefused { room, .. }) => {
            Incoming::UnknownRefusal(UnknownRefusal::Backfill { room })
        }
        Err(_) => Incoming::Undecodable,
    }
}

pub async fn recv_frame(socket: &mut Socket) -> Result<ServerFrame> {
    loop {
        if let Incoming::Frame(frame) = recv(socket).await? {
            return Ok(frame);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use super::*;

    #[test]
    fn a_frame_this_build_cannot_read_is_skipped_not_fatal() {
        // A router one version ahead, and outright garbage, must land in the
        // same place: a frame the caller can step over. Returning an error here
        // is what made one unreadable answer cost a reader every other
        // machine's rows.
        for hostile in [
            br#"{"kind":"somethingNewer","id":1}"#.as_slice(),
            br#"{"kind":"report","id":1,"host":"sandbank","state":{"kind":"answering"}}"#,
            b"not json at all",
            b"",
        ] {
            assert!(
                matches!(classify(hostile), Incoming::Undecodable),
                "expected Undecodable for {:?}",
                String::from_utf8_lossy(hostile)
            );
        }
    }

    fn room_json() -> &'static str {
        r#"{"project":"example-repo","scope":{"repo":"example-org/example-repo"}}"#
    }

    #[test]
    fn a_refusal_with_a_category_from_a_newer_router_is_kept_as_unknown() {
        let post = format!(
            r#"{{"type":"post_refused","room":{},"category":"slow_down_please"}}"#,
            room_json()
        );
        assert!(
            matches!(
                classify(post.as_bytes()),
                Incoming::UnknownRefusal(UnknownRefusal::Post { .. })
            ),
            "an unknown post refusal was dropped as undecodable"
        );
        let backfill = format!(
            r#"{{"type":"backfill_refused","room":{},"category":"rate_limited"}}"#,
            room_json()
        );
        assert!(matches!(
            classify(backfill.as_bytes()),
            Incoming::UnknownRefusal(UnknownRefusal::Backfill { .. })
        ));
    }

    #[test]
    fn a_known_refusal_category_still_decodes_as_the_real_frame() {
        let post = format!(
            r#"{{"type":"post_refused","room":{},"category":"rate_limited"}}"#,
            room_json()
        );
        assert!(matches!(
            classify(post.as_bytes()),
            Incoming::Frame(ServerFrame::PostRefused {
                category: sealmesh_proto::PostRefusal::RateLimited,
                ..
            })
        ));
    }

    #[test]
    fn the_tolerance_admits_a_new_category_word_and_nothing_else() {
        for hostile in [
            // A category that is not a word.
            format!(r#"{{"type":"post_refused","room":{},"category":{{"x":1}}}}"#, room_json()),
            format!(r#"{{"type":"post_refused","room":{},"category":7}}"#, room_json()),
            // A field riding along.
            format!(
                r#"{{"type":"post_refused","room":{},"category":"new","body":"hi"}}"#,
                room_json()
            ),
            // No category at all.
            format!(r#"{{"type":"backfill_refused","room":{}}}"#, room_json()),
            // A room that does not validate.
            r#"{"type":"post_refused","room":{"project":"Not Valid","scope":"project"},"category":"new"}"#
                .to_owned(),
            // Another frame with a strange field is not a refusal.
            format!(r#"{{"type":"joined","room":{},"category":"new"}}"#, room_json()),
        ] {
            assert!(
                matches!(classify(hostile.as_bytes()), Incoming::Undecodable),
                "tolerated {hostile}"
            );
        }
    }

    #[test]
    fn a_frame_this_build_understands_still_decodes() {
        let json = serde_json::to_string(&ServerFrame::Complete {
            id: sealmesh_proto::QueryId(7),
        })
        .unwrap();
        assert!(matches!(
            classify(json.as_bytes()),
            Incoming::Frame(ServerFrame::Complete {
                id: sealmesh_proto::QueryId(7)
            })
        ));
    }

    #[test]
    fn a_projects_reply_decodes_like_any_other_frame() {
        let json = serde_json::to_string(&ServerFrame::Projects { projects: vec![] }).unwrap();
        assert!(matches!(
            classify(json.as_bytes()),
            Incoming::Frame(ServerFrame::Projects { projects }) if projects.is_empty()
        ));
    }

    #[test]
    fn the_projects_timeout_is_bounded() {
        // Long enough to tolerate a slow router, short enough that a `who`
        // against an old router — which never answers `ClientFrame::Projects`
        // at all — falls back to ungrouped output rather than hanging.
        assert!(
            PROJECTS_TIMEOUT <= Duration::from_secs(10),
            "{PROJECTS_TIMEOUT:?}"
        );
    }

    #[tokio::test]
    async fn a_wait_that_never_resolves_is_abandoned_rather_than_awaited() {
        // Mirrors the old-router fallback in `request_projects`: a router
        // that does not recognise `ClientFrame::Projects` never replies, so
        // `wait_for_projects` would hang forever without a bound. This pins
        // the bound itself, independent of any real socket.
        let started = std::time::Instant::now();
        let never = std::future::pending::<()>();
        assert_eq!(within(Duration::from_millis(50), never).await, None);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the deadline did not fire: waited {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn a_wait_that_resolves_in_time_is_kept() {
        assert_eq!(within(Duration::from_secs(30), async { 7 }).await, Some(7));
    }

    #[tokio::test]
    async fn request_projects_against_a_router_that_never_replies_falls_back_to_none() {
        // The real fallback, not just the timeout constant and the generic
        // `within` helper in isolation: a live peer that completes the
        // WebSocket handshake, reads the `ClientFrame::Projects` request, and
        // then holds the connection open without ever answering it -- the
        // stand-in for a router built before this shipped.
        use futures_util::StreamExt as _;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = ws.next().await;
            std::future::pending::<()>().await
        });

        let (mut socket, _response) =
            connect_async_with_config(format!("ws://{addr}"), None, false)
                .await
                .unwrap();

        let started = std::time::Instant::now();
        let result = request_projects(&mut socket).await.unwrap();
        let elapsed = started.elapsed();

        assert_eq!(result, None);
        assert!(elapsed >= PROJECTS_TIMEOUT, "returned early: {elapsed:?}");
        assert!(
            elapsed < Duration::from_secs(10),
            "the deadline did not fire: waited {elapsed:?}"
        );

        server.abort();
    }
}
