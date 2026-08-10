use std::{fs, io, path::PathBuf};

use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{AppPaths, PathError};

/// Small disposable JSON cache used by application composition roots.
pub struct CacheStore {
    paths: AppPaths,
    entries: PathBuf,
}

impl CacheStore {
    pub fn open(paths: &AppPaths) -> Result<Self, CacheError> {
        paths.create_cache_root()?;
        let entries = paths.cache_root.join("entries");
        fs::create_dir_all(&entries).map_err(CacheError::Io)?;
        Ok(Self {
            paths: paths.clone(),
            entries,
        })
    }

    pub fn put(&self, namespace: &str, key: &str, value: &Value) -> Result<(), CacheError> {
        validate_key(namespace)?;
        validate_key(key)?;
        fs::write(self.path(namespace, key), serde_json::to_vec(value)?).map_err(CacheError::Io)
    }

    pub fn exists(&self, namespace: &str, key: &str) -> Result<bool, CacheError> {
        validate_key(namespace)?;
        validate_key(key)?;
        Ok(self.path(namespace, key).try_exists()?)
    }

    pub fn clear(&self) -> Result<(), CacheError> {
        if self.paths.cache_root.try_exists()? {
            fs::remove_dir_all(&self.paths.cache_root)?;
        }
        self.paths.create_cache_root()?;
        fs::create_dir_all(&self.entries)?;
        Ok(())
    }

    pub fn count(&self) -> Result<u64, CacheError> {
        Ok(
            fs::read_dir(&self.entries)?
                .try_fold(0_u64, |count, entry| entry.map(|_| count + 1))?,
        )
    }

    fn path(&self, namespace: &str, key: &str) -> PathBuf {
        let mut hasher = Sha256::new();
        hasher.update(namespace.len().to_le_bytes());
        hasher.update(namespace.as_bytes());
        hasher.update(key.as_bytes());
        let mut name = String::with_capacity(69);
        for byte in hasher.finalize() {
            use std::fmt::Write as _;
            write!(name, "{byte:02x}").expect("writing to a String cannot fail");
        }
        name.push_str(".json");
        self.entries.join(name)
    }
}

fn validate_key(value: &str) -> Result<(), CacheError> {
    if value.is_empty() || value.len() > 4096 || value.chars().any(char::is_control) {
        Err(CacheError::InvalidKey)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("invalid cache namespace or key")]
    InvalidKey,
    #[error("cache JSON serialization failed: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("cache filesystem operation failed: {0}")]
    Io(#[from] io::Error),
    #[error(transparent)]
    Path(#[from] PathError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clear_is_scoped_and_entries_survive_reopen() {
        let root = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(root.path());
        paths.create_config_root().unwrap();
        fs::write(paths.config_root.join("keep"), b"config").unwrap();

        let cache = CacheStore::open(&paths).unwrap();
        fs::write(paths.cache_root.join("stale"), b"cache").unwrap();
        cache
            .put("provider", "key", &serde_json::json!({"n": 1}))
            .unwrap();
        assert!(
            CacheStore::open(&paths)
                .unwrap()
                .exists("provider", "key")
                .unwrap()
        );

        cache.clear().unwrap();
        assert!(!cache.exists("provider", "key").unwrap());
        assert!(!paths.cache_root.join("stale").exists());
        assert_eq!(fs::read(paths.config_root.join("keep")).unwrap(), b"config");
    }
}
