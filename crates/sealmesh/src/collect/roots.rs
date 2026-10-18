use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Root(PathBuf);

impl Root {
    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn jobs_dir(&self) -> PathBuf {
        self.0.join("jobs")
    }

    pub fn sessions_dir(&self) -> PathBuf {
        self.0.join("sessions")
    }

    pub fn tasks_dir(&self, session_id: &str) -> PathBuf {
        self.0.join("tasks").join(session_id)
    }
}

pub fn discover(
    home: Option<&Path>,
    extra: &[PathBuf],
    config_dir_env: Option<&Path>,
) -> Vec<Root> {
    let mut candidates: Vec<PathBuf> = Vec::new();

    if let Some(dir) = config_dir_env {
        candidates.push(dir.to_path_buf());
    }
    if let Some(home) = home
        && let Ok(entries) = std::fs::read_dir(home)
    {
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with(".claude") {
                candidates.push(entry.path());
            }
        }
    }
    candidates.extend(extra.iter().cloned());

    let mut seen = BTreeSet::new();
    let mut roots = Vec::new();
    for candidate in candidates {
        let Ok(canonical) = candidate.canonicalize() else {
            continue;
        };
        if !canonical.join("jobs").is_dir() && !canonical.join("sessions").is_dir() {
            continue;
        }
        if seen.insert(canonical.clone()) {
            roots.push(Root(canonical));
        }
    }
    roots.sort();
    roots
}

pub fn discover_local(extra: &[PathBuf]) -> Vec<Root> {
    let env = std::env::var_os("SEALMESH_CLAUDE_CONFIG_DIR").map(PathBuf::from);
    discover(dirs::home_dir().as_deref(), extra, env.as_deref())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    struct Fake(PathBuf);

    impl Fake {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("sealmesh-roots-{name}"));
            std::fs::remove_dir_all(&dir).ok();
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn root(&self, name: &str, subdirs: &[&str]) -> PathBuf {
            let dir = self.0.join(name);
            std::fs::create_dir_all(&dir).unwrap();
            for sub in subdirs {
                std::fs::create_dir_all(dir.join(sub)).unwrap();
            }
            dir
        }
    }

    impl Drop for Fake {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).ok();
        }
    }

    #[test]
    fn both_config_roots_are_found_not_just_the_first() {
        let fake = Fake::new("both");
        let a = fake.root(".claude", &["jobs"]);
        let b = fake.root(".claude-example-org", &["jobs"]);

        let roots = discover(Some(&fake.0), &[], None);
        let found: BTreeSet<_> = roots.iter().map(|r| r.path().to_path_buf()).collect();
        assert!(found.contains(&a.canonicalize().unwrap()));
        assert!(found.contains(&b.canonicalize().unwrap()));
        assert_eq!(roots.len(), 2);
    }

    #[test]
    fn a_claude_path_with_neither_subdirectory_is_not_a_root() {
        let fake = Fake::new("nojobs");
        fake.root(".claude-empty", &[]);
        std::fs::write(fake.0.join(".claude.json"), "{}").unwrap();

        assert!(discover(Some(&fake.0), &[], None).is_empty());
    }

    #[test]
    fn a_root_holding_only_sessions_is_discovered() {
        let fake = Fake::new("sessionsonly");
        let a = fake.root(".claude", &["sessions"]);

        let roots = discover(Some(&fake.0), &[], None);
        assert_eq!(roots.len(), 1, "{roots:?}");
        assert_eq!(roots.first().unwrap().path(), a.canonicalize().unwrap());
    }

    #[test]
    fn a_root_holding_both_is_not_counted_twice() {
        let fake = Fake::new("bothsubdirs");
        fake.root(".claude", &["jobs", "sessions"]);

        assert_eq!(discover(Some(&fake.0), &[], None).len(), 1);
    }

    #[test]
    fn the_env_root_is_included_and_never_duplicated() {
        let fake = Fake::new("env");
        let a = fake.root(".claude-example-org", &["jobs"]);

        let roots = discover(Some(&fake.0), std::slice::from_ref(&a), Some(&a));
        assert_eq!(roots.len(), 1, "same root counted more than once");
        assert_eq!(roots.first().unwrap().path(), a.canonicalize().unwrap());
    }
}
