use std::{io::Read as _, path::Path};

pub const MAX_DIR_ENTRIES: usize = 2048;

pub fn read_capped(path: &Path, cap: u64) -> Option<Vec<u8>> {
    if !std::fs::metadata(path).ok()?.is_file() {
        return None;
    }

    let file = std::fs::File::open(path).ok()?;
    let mut bytes = Vec::new();
    file.take(cap.saturating_add(1))
        .read_to_end(&mut bytes)
        .ok()?;

    (u64::try_from(bytes.len()).unwrap_or(u64::MAX) <= cap).then_some(bytes)
}

pub fn is_path_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::path::PathBuf;

    use super::*;

    #[test]
    fn a_session_id_that_could_walk_out_of_the_root_is_refused() {
        for hostile in [
            "../../../../etc/ssl/certs",
            "/etc",
            "..",
            ".",
            "a/b",
            "sess\u{0}ion",
            "",
            &"a".repeat(65),
        ] {
            assert!(!is_path_segment(hostile), "accepted {hostile:?}");
        }
        assert!(is_path_segment("9f3c1a7e-4b2d-4e91-8a55-1c0de2f6b7aa"));
        assert!(is_path_segment("session_01"));
    }

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sealmesh-fs-{name}"));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_file_over_the_cap_is_refused_rather_than_read() {
        let dir = scratch("capped");
        let small = dir.join("small.json");
        let big = dir.join("big.json");
        std::fs::write(&small, vec![b'a'; 64]).unwrap();
        std::fs::write(&big, vec![b'a'; 4096]).unwrap();

        assert_eq!(read_capped(&small, 1024).map(|b| b.len()), Some(64));
        assert_eq!(read_capped(&big, 1024), None, "read past the cap");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_file_exactly_on_the_cap_is_kept() {
        let dir = scratch("exact");
        let path = dir.join("exact.json");
        std::fs::write(&path, vec![b'a'; 1024]).unwrap();

        assert_eq!(read_capped(&path, 1024).map(|b| b.len()), Some(1024));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[test]
    fn a_named_pipe_is_refused_instead_of_blocking_on_it() {
        let dir = scratch("fifo");
        let fifo = dir.join("state.json");
        let made = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !made {
            eprintln!("skipped: mkfifo unavailable");
            return;
        }

        assert_eq!(read_capped(&fifo, 1024 * 1024), None);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_directory_or_a_missing_file_reads_as_absent() {
        let dir = scratch("notafile");
        assert_eq!(read_capped(&dir, 1024 * 1024), None);
        assert_eq!(read_capped(&dir.join("nope.json"), 1024 * 1024), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_directory_cap_is_far_above_a_real_machine() {
        const { assert!(MAX_DIR_ENTRIES >= 512) };
    }
}
