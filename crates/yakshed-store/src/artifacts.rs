use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, File, FileTimes, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use sha2::{Digest, Sha256};
use thiserror::Error;
use yakshed_domain::{
    ArtifactId, ArtifactKind, ArtifactProvenance, ArtifactRecord, ContentDigest, RunId, WorkItemId,
};

use crate::AppPaths;

static NEXT_STAGING_FILE: AtomicU64 = AtomicU64::new(0);
static NEXT_QUARANTINE_FILE: AtomicU64 = AtomicU64::new(0);
static ARTIFACT_LOCK_REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<KeyedArtifactLocks>>>> =
    OnceLock::new();

#[derive(Default)]
struct KeyedArtifactLocks {
    locks: Mutex<HashMap<ContentDigest, Weak<Mutex<()>>>>,
}

impl KeyedArtifactLocks {
    fn get(&self, digest: &ContentDigest) -> Arc<Mutex<()>> {
        let mut locks = self.locks.lock().unwrap();
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(digest).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(digest.clone(), Arc::downgrade(&lock));
        lock
    }
}

fn shared_artifact_locks(root: &Path) -> Arc<KeyedArtifactLocks> {
    let registry = ARTIFACT_LOCK_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry.lock().unwrap();
    registry.retain(|_, locks| locks.strong_count() > 0);
    if let Some(locks) = registry.get(root).and_then(Weak::upgrade) {
        return locks;
    }
    let locks = Arc::new(KeyedArtifactLocks::default());
    registry.insert(root.to_owned(), Arc::downgrade(&locks));
    locks
}

/// Caller-owned metadata needed to publish one artifact body.
pub struct ArtifactMetadata {
    pub id: ArtifactId,
    pub work_item_id: WorkItemId,
    pub run_id: Option<RunId>,
    pub kind: ArtifactKind,
    pub media_type: String,
    pub provenance: ArtifactProvenance,
}

/// Time source used only for orphan grace-period decisions.
pub trait Clock {
    fn now(&self) -> SystemTime;
}

/// Wall-clock implementation for production artifact collection.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Content-addressed immutable artifact blob storage.
///
/// Stores over the same canonical root share process-global digest locks. Cross-process
/// exclusion is intentionally owned by YakShed's single-instance application guarantee.
pub struct ArtifactStore<C = SystemClock> {
    sha256_root: PathBuf,
    staging_root: PathBuf,
    quarantine_root: PathBuf,
    max_size: u64,
    clock: C,
    locks: Arc<KeyedArtifactLocks>,
}

impl ArtifactStore {
    /// Opens a store beneath an `AppPaths` data root that the application already created.
    pub fn new(paths: &AppPaths, max_size: u64) -> Result<Self, ArtifactError> {
        Self::with_clock(paths, max_size, SystemClock)
    }
}

impl<C: Clock> ArtifactStore<C> {
    pub fn with_clock(paths: &AppPaths, max_size: u64, clock: C) -> Result<Self, ArtifactError> {
        let artifacts_root = paths.data_root.join("artifacts");
        let sha256_root = artifacts_root.join("sha256");
        let staging_root = artifacts_root.join("staging");
        // Quarantine is disposable diagnostic debris, never metadata-owned durable state.
        let quarantine_root = artifacts_root.join("quarantine");
        if !fs::metadata(&paths.data_root)?.is_dir() {
            return Err(ArtifactError::InvalidInput(
                "artifact data root must be an existing directory",
            ));
        }
        for directory in [
            &artifacts_root,
            &sha256_root,
            &staging_root,
            &quarantine_root,
        ] {
            create_directory_durable(directory, sync_directory)?;
        }
        let canonical_root = fs::canonicalize(&artifacts_root)?;
        Ok(Self {
            sha256_root,
            staging_root,
            quarantine_root,
            max_size,
            clock,
            locks: shared_artifact_locks(&canonical_root),
        })
    }

    /// Publishes a durable blob before returning its caller-committable metadata record.
    ///
    /// The destination directory entry is synced before return. Publication also renews
    /// the blob's mtime lease; callers must commit the returned record within the grace
    /// period supplied to garbage collection.
    pub fn publish(
        &self,
        mut source: impl Read,
        metadata: ArtifactMetadata,
    ) -> Result<ArtifactRecord, ArtifactError> {
        if metadata.media_type.trim().is_empty() {
            return Err(ArtifactError::InvalidInput(
                "artifact media type cannot be empty",
            ));
        }

        let (mut staging_file, staging_path) = self.create_staging_file()?;
        let mut hasher = Sha256::new();
        let mut byte_len = 0_u64;
        let mut buffer = [0_u8; 8192];
        loop {
            let read = source.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            let Some(next_size) = byte_len
                .checked_add(read as u64)
                .filter(|size| *size <= self.max_size)
            else {
                drop(staging_file);
                fs::remove_file(&staging_path)?;
                return Err(ArtifactError::TooLarge {
                    max_size: self.max_size,
                });
            };
            byte_len = next_size;
            staging_file.write_all(&buffer[..read])?;
            hasher.update(&buffer[..read]);
        }
        staging_file.flush()?;
        staging_file.sync_all()?;
        drop(staging_file);

        let digest = digest_from_hasher(hasher);
        let destination = self.path_for_digest(&digest);
        let shard = destination.parent().expect("digest path has a parent");
        let digest_lock = self.locks.get(&digest);
        let _guard = digest_lock.lock().unwrap();
        create_directory_durable(shard, sync_directory)?;

        if destination.try_exists()? {
            match self.verify_file(&digest) {
                Ok(()) => fs::remove_file(&staging_path)?,
                Err(ArtifactError::DigestMismatch { .. }) => {
                    self.quarantine_corrupt(&digest, &destination)?;
                    promote_staging(
                        &staging_path,
                        &destination,
                        &self.sha256_root,
                        sync_directory,
                    )?;
                }
                Err(error) => return Err(error),
            }
        } else {
            promote_staging(
                &staging_path,
                &destination,
                &self.sha256_root,
                sync_directory,
            )?;
        }
        self.refresh_lease(&destination)?;

        Ok(ArtifactRecord {
            id: metadata.id,
            work_item_id: metadata.work_item_id,
            run_id: metadata.run_id,
            kind: metadata.kind,
            digest,
            byte_len,
            media_type: metadata.media_type,
            provenance: metadata.provenance,
        })
    }

    pub fn open(
        &self,
        digest: &ContentDigest,
        max_bytes: u64,
    ) -> Result<BoundedArtifactReader, ArtifactError> {
        let file = self.open_file(digest)?;
        let byte_len = file.metadata()?.len();
        if byte_len > max_bytes {
            return Err(ArtifactError::BoundExceeded {
                byte_len,
                max_bytes,
            });
        }
        Ok(BoundedArtifactReader {
            inner: file.take(max_bytes),
        })
    }

    pub fn verify(&self, expected: &ContentDigest) -> Result<(), ArtifactError> {
        let digest_lock = self.locks.get(expected);
        let _guard = digest_lock.lock().unwrap();
        self.verify_file(expected)
    }

    fn verify_file(&self, expected: &ContentDigest) -> Result<(), ArtifactError> {
        let mut file = self.open_file(expected)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; 8192];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        let actual = digest_from_hasher(hasher);
        if actual != *expected {
            return Err(ArtifactError::DigestMismatch {
                expected: expected.clone(),
                actual,
            });
        }
        Ok(())
    }

    /// Removes old unreferenced blobs and disposable staging/quarantine debris.
    ///
    /// Quarantine entries are never referenced and are retained by age alone.
    ///
    /// `grace` is also the metadata-commit lease: callers must commit a record within this
    /// duration after `publish` returns. Age is re-read under the digest lock immediately
    /// before deletion, so a concurrent successful publish renews the lease.
    pub fn collect_unreferenced(
        &self,
        referenced: &BTreeSet<ContentDigest>,
        grace: Duration,
    ) -> Result<usize, ArtifactError> {
        let now = self.clock.now();
        let mut removed = collect_old_debris(&self.staging_root, now, grace)?;
        removed += collect_old_debris(&self.quarantine_root, now, grace)?;

        for shard in fs::read_dir(&self.sha256_root)? {
            let shard = shard?;
            if !shard.file_type()?.is_dir() {
                continue;
            }
            for entry in fs::read_dir(shard.path())? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let Ok(digest) = entry.file_name().to_string_lossy().parse::<ContentDigest>()
                else {
                    continue;
                };
                if referenced.contains(&digest) {
                    continue;
                }
                let digest_lock = self.locks.get(&digest);
                let _guard = digest_lock.lock().unwrap();
                let path = self.path_for_digest(&digest);
                let metadata = match fs::metadata(&path) {
                    Ok(metadata) => metadata,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(ArtifactError::Io(error)),
                };
                if now
                    .duration_since(metadata.modified()?)
                    .is_ok_and(|age| age >= grace)
                {
                    fs::remove_file(path)?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    fn create_staging_file(&self) -> Result<(File, PathBuf), ArtifactError> {
        loop {
            let sequence = NEXT_STAGING_FILE.fetch_add(1, Ordering::Relaxed);
            let path = self
                .staging_root
                .join(format!("{}-{sequence}.tmp", std::process::id()));
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&path) {
                Ok(file) => {
                    set_private_file_permissions(&path)?;
                    return Ok((file, path));
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(ArtifactError::Io(error)),
            }
        }
    }

    fn path_for_digest(&self, digest: &ContentDigest) -> PathBuf {
        self.sha256_root
            .join(&digest.as_str()[..2])
            .join(digest.as_str())
    }

    fn open_file(&self, digest: &ContentDigest) -> Result<File, ArtifactError> {
        match File::open(self.path_for_digest(digest)) {
            Ok(file) => Ok(file),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Err(ArtifactError::NotFound(digest.clone()))
            }
            Err(error) => Err(ArtifactError::Io(error)),
        }
    }

    fn refresh_lease(&self, path: &Path) -> Result<(), ArtifactError> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        file.set_times(FileTimes::new().set_modified(self.clock.now()))?;
        file.sync_all()?;
        Ok(())
    }

    fn quarantine_corrupt(
        &self,
        digest: &ContentDigest,
        canonical: &Path,
    ) -> Result<(), ArtifactError> {
        loop {
            let sequence = NEXT_QUARANTINE_FILE.fetch_add(1, Ordering::Relaxed);
            let quarantine =
                self.quarantine_root
                    .join(format!("{}-{}-{sequence}", digest, std::process::id()));
            if quarantine.try_exists()? {
                continue;
            }
            match fs::rename(canonical, &quarantine) {
                Ok(()) => {
                    self.refresh_lease(&quarantine)?;
                    sync_directory(&self.quarantine_root)?;
                    sync_directory(canonical.parent().expect("digest path has a parent"))?;
                    return Ok(());
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(ArtifactError::Io(error)),
            }
        }
    }
}

/// Reader that cannot consume more bytes than the bound accepted by `ArtifactStore::open`.
pub struct BoundedArtifactReader {
    inner: io::Take<File>,
}

impl Read for BoundedArtifactReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buffer)
    }
}

fn collect_old_debris(
    root: &Path,
    now: SystemTime,
    grace: Duration,
) -> Result<usize, ArtifactError> {
    let mut removed = 0;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let modified = entry.metadata()?.modified()?;
        if now.duration_since(modified).is_ok_and(|age| age >= grace) {
            fs::remove_file(entry.path())?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn promote_staging(
    staging: &Path,
    destination: &Path,
    sha256_root: &Path,
    mut sync: impl FnMut(&Path) -> Result<(), ArtifactError>,
) -> Result<(), ArtifactError> {
    fs::rename(staging, destination)?;
    set_private_file_permissions(destination)?;
    sync(destination.parent().expect("digest path has a parent"))?;
    sync(sha256_root)?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), ArtifactError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn digest_from_hasher(hasher: Sha256) -> ContentDigest {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = hasher.finalize();
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    ContentDigest::new(encoded)
        .expect("SHA-256 always formats as 64 lowercase hexadecimal characters")
}

fn create_directory_durable(
    path: &Path,
    mut sync: impl FnMut(&Path) -> Result<(), ArtifactError>,
) -> Result<(), ArtifactError> {
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => {}
        Err(error) => return Err(ArtifactError::Io(error)),
    }
    set_private_directory_permissions(path)?;
    sync(path.parent().expect("managed directory has a parent"))
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<(), ArtifactError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<(), ArtifactError> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<(), ArtifactError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<(), ArtifactError> {
    Ok(())
}

/// Failures publishing, opening, verifying, or collecting artifact blobs.
#[derive(Debug, Error)]
pub enum ArtifactError {
    #[error("artifact exceeds configured maximum of {max_size} bytes")]
    TooLarge { max_size: u64 },
    #[error("artifact digest mismatch: expected {expected}, got {actual}")]
    DigestMismatch {
        expected: ContentDigest,
        actual: ContentDigest,
    },
    #[error("artifact blob not found: {0}")]
    NotFound(ContentDigest),
    #[error("artifact is {byte_len} bytes, exceeding reader bound {max_bytes}")]
    BoundExceeded { byte_len: u64, max_bytes: u64 },
    #[error("invalid artifact input: {0}")]
    InvalidInput(&'static str),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    #[test]
    fn sync_directory_syncs_an_open_directory_handle() {
        let temp = tempfile::tempdir().unwrap();

        sync_directory(temp.path()).unwrap();
    }

    #[test]
    fn promotion_syncs_directory_entries_after_rename() {
        let temp = tempfile::tempdir().unwrap();
        let sha256_root = temp.path().join("sha256");
        let shard = sha256_root.join("ab");
        fs::create_dir_all(&shard).unwrap();
        let staging = temp.path().join("staging");
        let destination = shard.join("abcdef");
        fs::write(&staging, b"blob").unwrap();
        let synced = RefCell::new(Vec::new());

        promote_staging(&staging, &destination, &sha256_root, |directory| {
            assert!(destination.exists(), "sync ran before rename");
            synced.borrow_mut().push(directory.to_owned());
            Ok(())
        })
        .unwrap();

        assert_eq!(*synced.borrow(), [shard, sha256_root]);
    }

    #[test]
    fn cold_start_syncs_each_new_directory_parent() {
        let temp = tempfile::tempdir().unwrap();
        let data_root = temp.path().join("data");
        let artifacts_root = data_root.join("artifacts");
        let sha256_root = artifacts_root.join("sha256");
        let staging_root = artifacts_root.join("staging");
        let quarantine_root = artifacts_root.join("quarantine");
        let shard = sha256_root.join("ab");
        fs::create_dir(&data_root).unwrap();
        let synced = RefCell::new(Vec::new());
        let mut record_sync = |directory: &Path| {
            synced.borrow_mut().push(directory.to_owned());
            Ok(())
        };

        for directory in [
            &artifacts_root,
            &sha256_root,
            &staging_root,
            &quarantine_root,
            &shard,
        ] {
            create_directory_durable(directory, &mut record_sync).unwrap();
        }
        let staging = staging_root.join("cold-start.tmp");
        let destination = shard.join("abcdef");
        fs::write(&staging, b"blob").unwrap();
        promote_staging(&staging, &destination, &sha256_root, &mut record_sync).unwrap();

        assert_eq!(
            *synced.borrow(),
            [
                data_root,
                artifacts_root.clone(),
                artifacts_root.clone(),
                artifacts_root,
                sha256_root.clone(),
                shard,
                sha256_root,
            ]
        );
    }

    #[test]
    fn stores_for_one_canonical_root_share_digest_locks() {
        let temp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();

        let first = shared_artifact_locks(&root);
        let second = shared_artifact_locks(&root);

        assert!(Arc::ptr_eq(&first, &second));
        let weak = Arc::downgrade(&first);
        drop(first);
        drop(second);
        assert!(weak.upgrade().is_none());
    }
}
