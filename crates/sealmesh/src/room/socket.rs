//! Where the daemon's local socket lives, and how it comes to be there.
//!
//! The socket is a trust boundary: whoever can connect can post to this
//! machine's rooms and read what arrived in them. So it is reachable by this
//! user alone, three ways over:
//!
//! - it sits in a directory of its own, mode 0700, owned by this user, which
//!   is checked rather than assumed when the directory already exists;
//! - the socket itself is 0600, and is bound under a temporary name and
//!   renamed into place only after its mode is set, so the published path
//!   never has wider permissions, not even for an instant;
//! - every connection's peer uid is checked on accept (see `server`).
//!
//! A socket file left behind by a daemon that crashed is replaced. Anything
//! else at that path — a symlink, a regular file, a directory, a socket that
//! another user owns, or a socket something is still listening on — is left
//! exactly where it is and the bind fails, saying why (rule 33).

use std::{
    io::ErrorKind,
    os::unix::{
        fs::{DirBuilderExt as _, FileTypeExt as _, MetadataExt as _, PermissionsExt as _},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

/// The longest socket path this daemon binds, in bytes. macOS's `sun_path`
/// holds 104 bytes including the terminating NUL; Linux's holds 108. The
/// smaller one decides, so a path that works on one laptop works on all.
pub const MAX_SOCKET_PATH_BYTES: usize = 103;

const SOCKET_NAME: &str = "daemon.sock";

/// Where the daemon listens: `$XDG_STATE_HOME/sealmesh/local/daemon.sock`,
/// or `~/.local/state/sealmesh/local/daemon.sock`.
pub fn default_path() -> Option<PathBuf> {
    crate::paths::local_socket_dir().map(|dir| dir.join(SOCKET_NAME))
}

/// Refuses a path too long for `sun_path`, before anything is created.
pub fn check_length(path: &Path) -> Result<()> {
    let bytes = path.as_os_str().len();
    if bytes > MAX_SOCKET_PATH_BYTES {
        bail!(
            "the local socket path {} is {bytes} bytes, over the {MAX_SOCKET_PATH_BYTES} a Unix \
             socket path may hold on macOS. Set XDG_STATE_HOME to a shorter directory.",
            path.display()
        );
    }
    Ok(())
}

/// The name a socket is bound under before it is renamed into place. No
/// longer than [`SOCKET_NAME`], so the length check on the final path covers
/// it too.
fn staging_name() -> String {
    format!("b{:05}.sock", std::process::id() % 100_000)
}

/// This process's effective uid, as the kernel reports it for one end of a
/// socket pair. Read without `unsafe` and without a dependency, and before
/// anything is created, so a directory's owner can be checked before a
/// socket is bound in it. Needs a running Tokio runtime, which the daemon
/// always has.
pub fn own_uid() -> Result<u32> {
    let (one, _other) =
        tokio::net::UnixStream::pair().context("opening a socket pair to read this user's uid")?;
    Ok(one
        .peer_cred()
        .context("reading this user's uid from a socket pair")?
        .uid())
}

/// Binds the daemon's socket at `path`. Returns the listener and the uid it
/// belongs to — this process's effective uid, which every accepted peer must
/// match.
pub fn bind(path: &Path) -> Result<(UnixListener, u32)> {
    bind_with(path, |_| {})
}

/// `bind`, with a look at the staging socket just before it is renamed into
/// place, so a test can check the order of what happens to it.
fn bind_with(path: &Path, before_rename: impl FnOnce(&Path)) -> Result<(UnixListener, u32)> {
    check_length(path)?;
    let Some(dir) = path.parent() else {
        bail!("the local socket path {} has no directory", path.display());
    };
    let staging = dir.join(staging_name());
    check_length(&staging)?;

    let uid = own_uid()?;
    prepare_dir(dir, uid)?;
    clear_leftovers(dir, uid)?;

    let listener = UnixListener::bind(&staging)
        .with_context(|| format!("binding the local socket at {}", staging.display()))?;
    match publish(&staging, path, uid, before_rename) {
        Ok(()) => Ok((listener, uid)),
        Err(error) => {
            drop(listener);
            let _ = std::fs::remove_file(&staging);
            Err(error)
        }
    }
}

/// Creates the socket's directory as 0700 if it is missing. An existing one
/// must be a real directory, not a symlink, and belong to `uid`; one with
/// wider permissions is narrowed. All of it before anything is bound there.
fn prepare_dir(dir: &Path, uid: u32) -> Result<()> {
    if let Some(parent) = dir.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    match std::fs::DirBuilder::new().mode(0o700).create(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e).with_context(|| format!("creating {}", dir.display())),
    }
    let meta =
        std::fs::symlink_metadata(dir).with_context(|| format!("reading {}", dir.display()))?;
    if !meta.file_type().is_dir() {
        bail!(
            "{} is not a directory (a symlink or a file is there), so the local socket will \
             not be placed in it",
            dir.display()
        );
    }
    if meta.uid() != uid {
        bail!(
            "{} belongs to another user, so the local socket will not be placed in it",
            dir.display()
        );
    }
    if meta.mode() & 0o077 != 0 {
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("narrowing {} to 0700", dir.display()))?;
    }
    Ok(())
}

/// Whether `name` is a staging name, as `staging_name` makes them.
fn is_staging_name(name: &str) -> bool {
    name.strip_prefix('b')
        .and_then(|rest| rest.strip_suffix(".sock"))
        .is_some_and(|digits| digits.len() == 5 && digits.bytes().all(|b| b.is_ascii_digit()))
}

/// Removes staging sockets left by a daemon that crashed mid-bind — any
/// daemon, not only one with this pid. Only in the directory `prepare_dir`
/// has just made this user's own and 0700, and only an entry that is a
/// socket, belongs to `uid`, and has nothing listening on it. Anything else
/// is left alone.
fn clear_leftovers(dir: &Path, uid: u32) -> Result<()> {
    let entries = std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))?;
    // Bounded: a directory only this user can write holds only what this
    // user's daemons put there.
    for entry in entries.take(256).flatten() {
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !is_staging_name(&name) {
            continue;
        }
        let path = entry.path();
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.file_type().is_socket() || meta.uid() != uid {
            continue;
        }
        if matches!(UnixStream::connect(&path), Err(e) if e.kind() == ErrorKind::ConnectionRefused)
        {
            std::fs::remove_file(&path)
                .with_context(|| format!("removing a leftover {}", path.display()))?;
        }
    }
    Ok(())
}

/// Narrows the staging socket to 0600, then renames it into place: the
/// published path never has wider permissions.
fn publish(staging: &Path, path: &Path, uid: u32, before_rename: impl FnOnce(&Path)) -> Result<()> {
    let meta = std::fs::symlink_metadata(staging)
        .with_context(|| format!("reading {}", staging.display()))?;
    if !meta.file_type().is_socket() || meta.uid() != uid {
        bail!("{} is not the socket just bound", staging.display());
    }
    std::fs::set_permissions(staging, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("narrowing {} to 0600", staging.display()))?;

    check_replaceable(path, uid)?;
    before_rename(staging);
    std::fs::rename(staging, path)
        .with_context(|| format!("moving the local socket into place at {}", path.display()))?;
    Ok(())
}

/// Whatever is at `path` may be replaced only if it is this user's socket and
/// nothing is listening on it: a daemon that crashed left it.
fn check_replaceable(path: &Path, uid: u32) -> Result<()> {
    let meta = match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
        Ok(meta) => meta,
    };
    let kind = meta.file_type();
    if kind.is_symlink() {
        bail!(
            "{} is a symlink, not a socket this daemon left, so it is left alone",
            path.display()
        );
    }
    if !kind.is_socket() {
        bail!(
            "{} exists and is not a socket, so it is left alone",
            path.display()
        );
    }
    if meta.uid() != uid {
        bail!(
            "{} belongs to another user, so it is left alone",
            path.display()
        );
    }
    match UnixStream::connect(path) {
        Ok(_) => bail!(
            "another sealmesh daemon is already serving {}. Stop it first; two daemons \
             on one machine replace each other at the router",
            path.display()
        ),
        Err(e) if e.kind() == ErrorKind::ConnectionRefused => Ok(()),
        Err(e) => Err(e).with_context(|| format!("checking whether {} is live", path.display())),
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

    /// A fresh directory under the system temp dir. Kept short: macOS's temp
    /// dir alone is about 50 bytes of the 103.
    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("aw{}{tag}", std::process::id() % 100_000));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn mode(path: &Path) -> u32 {
        std::fs::symlink_metadata(path).unwrap().mode() & 0o777
    }

    #[tokio::test]
    async fn the_socket_is_0600_in_a_0700_directory_and_connectable() {
        let root = scratch("m");
        let path = root.join("local").join(SOCKET_NAME);
        let (listener, uid) = bind(&path).unwrap();
        assert_eq!(mode(path.parent().unwrap()), 0o700);
        assert_eq!(mode(&path), 0o600);
        assert!(
            std::fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_socket()
        );
        assert_eq!(std::fs::symlink_metadata(&path).unwrap().uid(), uid);
        // Renaming into place kept the listener reachable at the final path.
        let _client = UnixStream::connect(&path).unwrap();
        let (_conn, _) = listener.accept().unwrap();
        // Nothing but the socket is left in the directory.
        let names: Vec<_> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, vec![std::ffi::OsString::from(SOCKET_NAME)]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn an_existing_directory_with_wider_permissions_is_narrowed() {
        let root = scratch("w");
        let dir = root.join("local");
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let (_listener, _) = bind(&dir.join(SOCKET_NAME)).unwrap();
        assert_eq!(mode(&dir), 0o700);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_stale_socket_from_a_crashed_daemon_is_replaced() {
        let root = scratch("s");
        let path = root.join("local").join(SOCKET_NAME);
        let (listener, _) = bind(&path).unwrap();
        // The daemon dies: the file stays, nothing listens.
        drop(listener);
        assert!(path.exists());
        let (listener, _) = bind(&path).unwrap();
        let _client = UnixStream::connect(&path).unwrap();
        let (_conn, _) = listener.accept().unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_live_socket_is_not_taken_over() {
        let root = scratch("l");
        let path = root.join("local").join(SOCKET_NAME);
        let (_listener, _) = bind(&path).unwrap();
        let error = format!("{:#}", bind(&path).unwrap_err());
        assert!(error.contains("already serving"), "{error}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_symlink_or_a_file_at_the_path_is_never_removed() {
        let root = scratch("y");
        let dir = root.join("local");
        std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
        let path = dir.join(SOCKET_NAME);

        let target = root.join("precious");
        std::fs::write(&target, b"keep me").unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        let error = format!("{:#}", bind(&path).unwrap_err());
        assert!(error.contains("symlink"), "{error}");
        assert!(
            std::fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"keep me");

        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"a regular file").unwrap();
        let error = format!("{:#}", bind(&path).unwrap_err());
        assert!(error.contains("not a socket"), "{error}");
        assert_eq!(std::fs::read(&path).unwrap(), b"a regular file");
        // The staging socket did not survive the refusal.
        let names: Vec<_> = std::fs::read_dir(&dir).unwrap().collect();
        assert_eq!(names.len(), 1);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_symlinked_directory_is_refused() {
        let root = scratch("d");
        let real = root.join("real");
        std::fs::create_dir(&real).unwrap();
        let dir = root.join("local");
        std::os::unix::fs::symlink(&real, &dir).unwrap();
        let error = format!("{:#}", bind(&dir.join(SOCKET_NAME)).unwrap_err());
        assert!(error.contains("not a directory"), "{error}");
        assert_eq!(std::fs::read_dir(&real).unwrap().count(), 0);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn the_longest_path_allowed_binds_and_one_byte_more_is_refused_clearly() {
        let root = scratch("x");
        let base = root.join("local");
        // Pad the directory name so the whole socket path is exactly the limit.
        let fixed = base.as_os_str().len() + 1 + SOCKET_NAME.len();
        assert!(
            fixed < MAX_SOCKET_PATH_BYTES,
            "temp dir too long for this test"
        );
        let pad = MAX_SOCKET_PATH_BYTES - fixed;
        let dir = PathBuf::from(format!("{}{}", base.display(), "p".repeat(pad)));
        let path = dir.join(SOCKET_NAME);
        assert_eq!(path.as_os_str().len(), MAX_SOCKET_PATH_BYTES);
        let (listener, _) = bind(&path).unwrap();
        let _client = UnixStream::connect(&path).unwrap();
        let (_conn, _) = listener.accept().unwrap();

        let over =
            PathBuf::from(format!("{}{}", base.display(), "p".repeat(pad + 1))).join(SOCKET_NAME);
        let error = format!("{:#}", bind(&over).unwrap_err());
        assert!(error.contains("104") || error.contains("103"), "{error}");
        assert!(error.contains("XDG_STATE_HOME"), "{error}");
        assert!(
            !over.parent().unwrap().exists(),
            "created something before refusing"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn the_staging_socket_is_narrowed_before_it_is_renamed_into_place() {
        let root = scratch("o");
        let path = root.join("local").join(SOCKET_NAME);
        let mut seen = None;
        let (_listener, _) = bind_with(&path, |staging| {
            seen = Some((mode(staging), path.exists()));
        })
        .unwrap();
        assert_eq!(
            seen,
            Some((0o600, false)),
            "the socket was not 0600 before the rename, or was already published"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_leftover_staging_socket_from_a_crashed_daemon_is_cleared_and_nothing_else() {
        let root = scratch("c");
        let dir = root.join("local");
        std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();

        // A crashed daemon's staging socket: ours, a socket, nothing listening.
        let stale = dir.join("b99999.sock");
        drop(UnixListener::bind(&stale).unwrap());
        // Another daemon mid-bind: still listening.
        let live = dir.join("b99998.sock");
        let _live = UnixListener::bind(&live).unwrap();
        // Something with a staging name that is not a socket.
        let file = dir.join("b99997.sock");
        std::fs::write(&file, b"not a socket").unwrap();
        // A socket without a staging name.
        let other = dir.join("other.sock");
        drop(UnixListener::bind(&other).unwrap());
        // A symlink with a staging name, to a stale socket elsewhere:
        // connecting through it is refused, but it is not a socket.
        let elsewhere = root.join("elsewhere.sock");
        drop(UnixListener::bind(&elsewhere).unwrap());
        let link = dir.join("b99996.sock");
        std::os::unix::fs::symlink(&elsewhere, &link).unwrap();

        let (_listener, _) = bind(&dir.join(SOCKET_NAME)).unwrap();
        assert!(
            !stale.exists(),
            "the crashed daemon's staging socket was kept"
        );
        assert!(live.exists(), "a live staging socket was removed");
        assert_eq!(std::fs::read(&file).unwrap(), b"not a socket");
        assert!(std::fs::symlink_metadata(&other).is_ok());
        assert!(
            std::fs::symlink_metadata(&link).is_ok(),
            "a symlink was removed as if it were a staging socket"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn staging_names_are_recognised_exactly() {
        assert!(is_staging_name(&staging_name()));
        assert!(is_staging_name("b00001.sock"));
        for other in [
            "daemon.sock",
            "b1.sock",
            "b000001.sock",
            "bxxxxx.sock",
            "b00001.sockx",
        ] {
            assert!(!is_staging_name(other), "{other}");
        }
    }

    #[tokio::test]
    async fn own_uid_is_the_owner_of_what_this_process_creates() {
        let root = scratch("i");
        let file = root.join("probe");
        std::fs::write(&file, b"").unwrap();
        assert_eq!(own_uid().unwrap(), std::fs::metadata(&file).unwrap().uid());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn the_default_path_fits_for_an_ordinary_home() {
        let path = PathBuf::from("/Users/a-rather-long-username-here/.local/state")
            .join("sealmesh/local")
            .join(SOCKET_NAME);
        check_length(&path).unwrap();
    }

    #[test]
    fn the_staging_name_is_never_longer_than_the_final_one() {
        assert!(staging_name().len() <= SOCKET_NAME.len());
    }
}
