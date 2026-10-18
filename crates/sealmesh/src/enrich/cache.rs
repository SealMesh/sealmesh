use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use jiff::Timestamp;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Debug, Serialize, Deserialize)]
struct Entry<T> {
    fetched_at: Timestamp,
    value: T,
}

#[derive(Debug)]
pub struct Cache<T> {
    path: Option<PathBuf>,
    ttl_secs: i64,
    entries: BTreeMap<String, Entry<T>>,
}

impl<T: Serialize + DeserializeOwned + Clone> Cache<T> {
    pub fn load(name: &str, ttl_secs: i64) -> Self {
        let path =
            crate::paths::cache_home().map(|d| d.join("sealmesh").join(format!("{name}.json")));
        let entries = path
            .as_ref()
            .and_then(|p| std::fs::read(p).ok())
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default();
        Self {
            path,
            ttl_secs,
            entries,
        }
    }

    pub fn get(&self, key: &str, now: Timestamp) -> Option<T> {
        let entry = self.entries.get(key)?;
        let age = (now - entry.fetched_at).get_seconds();
        (age >= 0 && age < self.ttl_secs).then(|| entry.value.clone())
    }

    pub fn put(&mut self, key: String, value: T, now: Timestamp) {
        self.entries.insert(
            key,
            Entry {
                fetched_at: now,
                value,
            },
        );
    }

    pub fn put_for(&mut self, key: String, value: T, now: Timestamp, ttl_secs: i64) {
        let backdated = now
            .checked_sub(jiff::SignedDuration::from_secs(
                self.ttl_secs.saturating_sub(ttl_secs).max(0),
            ))
            .unwrap_or(now);
        self.put(key, value, backdated);
    }

    pub fn save(&mut self, now: Timestamp) {
        self.entries
            .retain(|_, entry| (now - entry.fetched_at).get_seconds() < self.ttl_secs);

        let Some(path) = &self.path else { return };
        let Some(parent) = path.parent() else { return };
        if create_private_dir(parent).is_err() {
            return;
        }
        let Ok(bytes) = serde_json::to_vec(&self.entries) else {
            return;
        };
        let tmp = path.with_extension("tmp");
        if write_private(&tmp, &bytes).is_ok() && std::fs::rename(&tmp, path).is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
    }
}

fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::{io::Write as _, os::unix::fs::OpenOptionsExt as _};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(bytes)
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, bytes)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    #[test]
    fn an_entry_expires_at_the_ttl() {
        let mut cache: Cache<u32> = Cache {
            path: None,
            ttl_secs: 60,
            entries: BTreeMap::new(),
        };
        let t0 = ts("2026-08-10T19:00:00Z");
        cache.put("k".to_owned(), 7, t0);

        assert_eq!(cache.get("k", ts("2026-08-10T19:00:59Z")), Some(7));
        assert_eq!(cache.get("k", ts("2026-08-10T19:01:00Z")), None);
    }

    #[test]
    fn an_entry_from_the_future_is_not_trusted() {
        let mut cache: Cache<u32> = Cache {
            path: None,
            ttl_secs: 60,
            entries: BTreeMap::new(),
        };
        cache.put("k".to_owned(), 7, ts("2026-08-10T20:00:00Z"));
        assert_eq!(cache.get("k", ts("2026-08-10T19:00:00Z")), None);
    }
}
