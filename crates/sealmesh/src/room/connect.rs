//! The client side of the daemon's local socket, shared by `sealmesh chat`,
//! the MCP room tools and the hook.
//!
//! One request, one response, under one deadline that covers connecting,
//! writing and reading. The response is capped in bytes before it is parsed
//! (rules 42, 44), and a response that does not decode is an error that names
//! no bytes (rule 14). Nothing here holds or sends a credential: the daemon
//! talks to the router with its own token, and this socket never sees it.
//!
//! A daemon that is not running, or has chat off, answers quickly either way:
//! `NotRunning` when nothing listens, `Response::Disabled` when chat is off.
//! Nothing here hangs.

use std::{
    io::ErrorKind,
    os::unix::fs::PermissionsExt as _,
    path::{Path, PathBuf},
    time::Duration,
};

use tokio::{io::AsyncWriteExt as _, net::UnixStream};

use super::{
    local::{self, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, Request, Response},
    server::read_line,
    socket,
};

/// Longer than the longest the daemon can take to answer — its read deadline,
/// then the longer of its wait on the router and the longest tail wait, then
/// its write deadline — so a slow router reads as the daemon's own
/// `timed_out`, and a tail of the longest legal wait is never cut off here.
/// Pinned against `server::Limits::default().longest_answer()` by a test.
pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(25);

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("no home directory to find the daemon's socket under")]
    NoHome,
    #[error("the sealmesh daemon is not running (nothing is listening at {0})")]
    NotRunning(PathBuf),
    #[error(
        "{0} is reachable by other users or is not a directory, so its socket is not trusted. \
         Restart the daemon, which narrows it to 0700"
    )]
    Unsafe(PathBuf),
    #[error("the daemon did not answer within {0:?}")]
    TimedOut(Duration),
    #[error("the daemon's answer was over the size limit")]
    TooLarge,
    #[error(
        "the daemon closed the connection without answering: it is at its limit of local \
         connections, or did not accept this process as the same user. Try again shortly"
    )]
    Refused,
    #[error(
        "the daemon's answer could not be read (line {line}, column {column}). It may be \
         running an older build: restart it"
    )]
    Protocol { line: usize, column: usize },
    #[error("talking to the daemon: {0}")]
    Io(#[source] std::io::Error),
}

/// Sends `request` to the daemon at its usual path.
pub async fn request_default(
    request: &Request,
    deadline: Duration,
) -> Result<Response, ConnectError> {
    let path = socket::default_path().ok_or(ConnectError::NoHome)?;
    self::request(&path, request, deadline).await
}

/// Sends `request` to the daemon listening at `path`.
pub async fn request(
    path: &Path,
    request: &Request,
    deadline: Duration,
) -> Result<Response, ConnectError> {
    let bytes = request_bytes(path, request, deadline).await?;
    local::decode::<Response>(&bytes, MAX_RESPONSE_BYTES).map_err(|e| match e {
        local::LocalError::Json { line, column } => ConnectError::Protocol { line, column },
        local::LocalError::TooLarge(_) | local::LocalError::ConsumerKey => ConnectError::TooLarge,
    })
}

/// Sends `request` to the daemon listening at `path` and returns its raw
/// answer bytes (newline stripped), capped and bounded by `deadline` exactly
/// as [`request`] is, but not decoded into a [`Response`].
///
/// For a caller that needs to decode part of the answer more leniently than
/// [`local::decode`] does — `chat`'s `Tail` poll decodes each message in a
/// batch on its own, so one that does not decode renders as an error in
/// place rather than losing the rest of the batch (rule 39) — while every
/// other shape still goes through the same strict, closed decode
/// [`request`] uses.
pub async fn request_bytes(
    path: &Path,
    request: &Request,
    deadline: Duration,
) -> Result<Vec<u8>, ConnectError> {
    check_dir(path)?;
    let line = local::encode(request, MAX_REQUEST_BYTES).map_err(|_| ConnectError::TooLarge)?;
    match tokio::time::timeout(deadline, exchange_bytes(path, &line)).await {
        Ok(result) => result,
        Err(_) => Err(ConnectError::TimedOut(deadline)),
    }
}

/// The socket's directory must be this user's alone. Anyone else able to
/// write there could have put their own socket in its place.
fn check_dir(path: &Path) -> Result<(), ConnectError> {
    let Some(dir) = path.parent() else {
        return Err(ConnectError::Unsafe(path.to_path_buf()));
    };
    match std::fs::symlink_metadata(dir) {
        Err(e) if e.kind() == ErrorKind::NotFound => {
            Err(ConnectError::NotRunning(path.to_path_buf()))
        }
        Err(e) => Err(ConnectError::Io(e)),
        Ok(meta) if !meta.file_type().is_dir() || meta.permissions().mode() & 0o077 != 0 => {
            Err(ConnectError::Unsafe(dir.to_path_buf()))
        }
        Ok(_) => Ok(()),
    }
}

async fn exchange_bytes(path: &Path, line: &[u8]) -> Result<Vec<u8>, ConnectError> {
    let mut stream = match UnixStream::connect(path).await {
        Ok(stream) => stream,
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::ConnectionRefused) => {
            return Err(ConnectError::NotRunning(path.to_path_buf()));
        }
        Err(e) => return Err(ConnectError::Io(e)),
    };
    // A daemon that refuses a connection — over its cap, or a peer it will
    // not serve — closes it unanswered, which reads here as a reset, a broken
    // pipe, or an empty answer depending on timing and platform.
    let refused = |e: std::io::Error| match e.kind() {
        ErrorKind::ConnectionReset | ErrorKind::BrokenPipe | ErrorKind::UnexpectedEof => {
            ConnectError::Refused
        }
        _ => ConnectError::Io(e),
    };
    stream.write_all(line).await.map_err(refused)?;
    let (read, _write) = stream.into_split();
    let Some(bytes) = read_line(read, MAX_RESPONSE_BYTES).await.map_err(refused)? else {
        return Err(ConnectError::TooLarge);
    };
    if bytes.is_empty() {
        return Err(ConnectError::Refused);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use std::os::unix::fs::DirBuilderExt as _;

    use tokio::{io::AsyncReadExt as _, net::UnixListener};

    use super::*;

    fn scratch(tag: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("awc{}{tag}", std::process::id() % 100_000));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let dir = root.join("local");
        std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
        dir.join("daemon.sock")
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_dir_all(path.parent().unwrap().parent().unwrap());
    }

    #[test]
    fn the_default_deadline_outlasts_the_longest_legal_answer() {
        let longest = crate::room::server::Limits::default().longest_answer();
        assert!(
            DEFAULT_DEADLINE > longest,
            "{DEFAULT_DEADLINE:?} would cut off an answer that takes {longest:?}"
        );
    }

    #[tokio::test]
    async fn nothing_listening_is_not_running_and_answers_at_once() {
        let path = scratch("a");
        let started = std::time::Instant::now();
        let error = request(&path, &Request::Status {}, DEFAULT_DEADLINE)
            .await
            .unwrap_err();
        assert!(matches!(error, ConnectError::NotRunning(_)), "{error}");
        assert!(started.elapsed() < Duration::from_secs(1));
        cleanup(&path);
    }

    #[tokio::test]
    async fn a_directory_others_can_reach_is_not_trusted() {
        let path = scratch("b");
        std::fs::set_permissions(
            path.parent().unwrap(),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        let error = request(&path, &Request::Status {}, DEFAULT_DEADLINE)
            .await
            .unwrap_err();
        assert!(matches!(error, ConnectError::Unsafe(_)), "{error}");
        cleanup(&path);
    }

    #[tokio::test]
    async fn a_daemon_that_never_answers_is_bounded_by_the_deadline() {
        let path = scratch("c");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let started = std::time::Instant::now();
        let error = request(&path, &Request::Status {}, Duration::from_millis(200))
            .await
            .unwrap_err();
        assert!(matches!(error, ConnectError::TimedOut(_)), "{error}");
        assert!(started.elapsed() < Duration::from_secs(2));
        server.abort();
        cleanup(&path);
    }

    #[tokio::test]
    async fn an_oversized_answer_is_refused_before_it_is_parsed() {
        let path = scratch("d");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 64];
            let _ = stream.read(&mut request).await;
            let huge = vec![b' '; MAX_RESPONSE_BYTES + 10];
            let _ = stream.write_all(&huge).await;
        });
        let error = request(&path, &Request::Status {}, DEFAULT_DEADLINE)
            .await
            .unwrap_err();
        assert!(matches!(error, ConnectError::TooLarge), "{error}");
        server.abort();
        cleanup(&path);
    }

    #[tokio::test]
    async fn request_bytes_refuses_an_oversized_answer_too() {
        // `request_bytes` shares `exchange_bytes` with `request`, but it is
        // a separate public entry point (chat's lenient `Tail` decode uses
        // it directly) and deserves its own guard against a refactor that
        // decouples the two and drops the cap for one of them.
        let path = scratch("d2");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 64];
            let _ = stream.read(&mut request).await;
            let huge = vec![b' '; MAX_RESPONSE_BYTES + 10];
            let _ = stream.write_all(&huge).await;
        });
        let error = request_bytes(&path, &Request::Status {}, DEFAULT_DEADLINE)
            .await
            .unwrap_err();
        assert!(matches!(error, ConnectError::TooLarge), "{error}");
        server.abort();
        cleanup(&path);
    }

    #[tokio::test]
    async fn an_answer_that_does_not_decode_names_no_bytes() {
        let path = scratch("e");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 64];
            let _ = stream.read(&mut request).await;
            let _ = stream
                .write_all(b"{\"result\":\"ghp_leakedvalue\"}\n")
                .await;
        });
        let error = request(&path, &Request::Status {}, DEFAULT_DEADLINE)
            .await
            .unwrap_err()
            .to_string();
        assert!(!error.contains("ghp_leakedvalue"), "{error}");
        server.abort();
        cleanup(&path);
    }
}
