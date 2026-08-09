use std::{
    collections::{BTreeSet, HashMap, HashSet},
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
static ARTIFACT_STATE_REGISTRY: OnceLock<Mutex<HashMap<PathBuf, Weak<SharedArtifactState>>>> =
    OnceLock::new();

#[derive(Default)]
struct SharedArtifactState {
    locks: Mutex<HashMap<ContentDigest, Weak<Mutex<()>>>>,
    active_staging: Mutex<HashSet<PathBuf>>,
}

impl SharedArtifactState {
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

    fn register_staging(self: &Arc<Self>, path: PathBuf) -> ActiveStaging {
        self.active_staging.lock().unwrap().insert(path.clone());
        ActiveStaging {
            state: Arc::clone(self),
            path,
        }
    }

    fn is_staging_active(&self, path: &Path) -> bool {
        self.active_staging.lock().unwrap().contains(path)
    }
}

struct ActiveStaging {
    state: Arc<SharedArtifactState>,
    path: PathBuf,
}

impl Drop for ActiveStaging {
    fn drop(&mut self) {
        self.state.active_staging.lock().unwrap().remove(&self.path);
    }
}

fn shared_artifact_state(root: &Path) -> Arc<SharedArtifactState> {
    let registry = ARTIFACT_STATE_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry.lock().unwrap();
    registry.retain(|_, state| state.strong_count() > 0);
    if let Some(state) = registry.get(root).and_then(Weak::upgrade) {
        return state;
    }
    let state = Arc::new(SharedArtifactState::default());
    registry.insert(root.to_owned(), Arc::downgrade(&state));
    state
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
    state: Arc<SharedArtifactState>,
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
            sha256_root: canonical_root.join("sha256"),
            staging_root: canonical_root.join("staging"),
            quarantine_root: canonical_root.join("quarantine"),
            max_size,
            clock,
            state: shared_artifact_state(&canonical_root),
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

        let (mut staging_file, staging_path, _active_staging) = self.create_staging_file()?;
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
        let digest_lock = self.state.get(&digest);
        let _guard = digest_lock.lock().unwrap();
        create_directory_durable(shard, sync_directory)?;

        if destination.try_exists()? {
            match self.verify_file(&digest) {
                Ok(()) => fs::remove_file(&staging_path)?,
                Err(ArtifactError::DigestMismatch { .. }) => {
                    self.replace_corrupt_blob(&digest, &staging_path, &destination, |_| {})?;
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
        let digest_lock = self.state.get(expected);
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
    /// Quarantine entries are never referenced and are retained by age alone. Active
    /// staging files are process-owned and skipped regardless of age.
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
        let mut removed = self.collect_old_staging(now, grace)?;
        removed += self.collect_old_quarantine(now, grace)?;

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
                let digest_lock = self.state.get(&digest);
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

    fn collect_old_staging(
        &self,
        now: SystemTime,
        grace: Duration,
    ) -> Result<usize, ArtifactError> {
        let mut removed = 0;
        for entry in fs::read_dir(&self.staging_root)? {
            let entry = entry?;
            let path = entry.path();
            if !entry.file_type()?.is_file() || self.state.is_staging_active(&path) {
                continue;
            }
            if is_old(&entry.metadata()?, now, grace)? {
                fs::remove_file(path)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn collect_old_quarantine(
        &self,
        now: SystemTime,
        grace: Duration,
    ) -> Result<usize, ArtifactError> {
        let mut removed = 0;
        for entry in fs::read_dir(&self.quarantine_root)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let digest_lock = entry
                .file_name()
                .to_str()
                .and_then(|name| name.split('-').next())
                .and_then(|digest| digest.parse::<ContentDigest>().ok())
                .map(|digest| self.state.get(&digest));
            let _guard = digest_lock.as_ref().map(|lock| lock.lock().unwrap());
            let path = entry.path();
            let metadata = match fs::metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(ArtifactError::Io(error)),
            };
            if is_old(&metadata, now, grace)? {
                fs::remove_file(path)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn create_staging_file(&self) -> Result<(File, PathBuf, ActiveStaging), ArtifactError> {
        loop {
            let sequence = NEXT_STAGING_FILE.fetch_add(1, Ordering::Relaxed);
            let path = self
                .staging_root
                .join(format!("{}-{sequence}.tmp", std::process::id()));
            let active = self.state.register_staging(path.clone());
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
                    return Ok((file, path, active));
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

    fn replace_corrupt_blob(
        &self,
        digest: &ContentDigest,
        staging: &Path,
        canonical: &Path,
        after_quarantine: impl FnOnce(&Path),
    ) -> Result<(), ArtifactError> {
        let quarantine = self.quarantine_corrupt(digest, canonical)?;
        after_quarantine(&quarantine);
        promote_staging(staging, canonical, &self.sha256_root, sync_directory)
    }

    fn quarantine_corrupt(
        &self,
        digest: &ContentDigest,
        canonical: &Path,
    ) -> Result<PathBuf, ArtifactError> {
        self.refresh_lease(canonical)?;
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
                    sync_directory(&self.quarantine_root)?;
                    sync_directory(canonical.parent().expect("digest path has a parent"))?;
                    return Ok(quarantine);
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

fn is_old(
    metadata: &fs::Metadata,
    now: SystemTime,
    grace: Duration,
) -> Result<bool, ArtifactError> {
    Ok(now
        .duration_since(metadata.modified()?)
        .is_ok_and(|age| age >= grace))
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

/// Flushes a directory entry on Unix, where directory handles support `fsync`.
#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), ArtifactError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

/// Windows' standard library exposes no directory-handle `FlushFileBuffers` equivalent.
/// Calls remain unconditional, but this accepted platform limitation is a no-op on Windows.
#[cfg(windows)]
fn sync_directory(_path: &Path) -> Result<(), ArtifactError> {
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
    #[cfg(unix)]
    use std::cell::RefCell;
    use std::{sync::mpsc, thread};

    use super::*;

    #[cfg(unix)]
    #[test]
    fn sync_directory_syncs_an_open_directory_handle() {
        let temp = tempfile::tempdir().unwrap();

        sync_directory(temp.path()).unwrap();
    }

    #[cfg(unix)]
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

    #[cfg(unix)]
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

        let first = shared_artifact_state(&root);
        let second = shared_artifact_state(&root);

        assert!(Arc::ptr_eq(&first, &second));
        let weak = Arc::downgrade(&first);
        drop(first);
        drop(second);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn quarantine_gc_waits_for_corrupt_repair_across_store_handles() {
        let temp = tempfile::tempdir().unwrap();
        let paths = AppPaths::for_test(temp.path());
        paths.create_data_root().unwrap();
        let repairer = ArtifactStore::new(&paths, 1024).unwrap();
        let collector = ArtifactStore::new(&paths, 1024).unwrap();
        let record = repairer
            .publish(
                &b"repair race"[..],
                ArtifactMetadata {
                    id: "0193f26e-7a72-7d42-bf77-0de14c4cc240".parse().unwrap(),
                    work_item_id: "0193f26e-7a72-7d42-bf77-0de14c4cc241".parse().unwrap(),
                    run_id: None,
                    kind: ArtifactKind::Plan,
                    media_type: "text/plain".to_owned(),
                    provenance: ArtifactProvenance::new("test").unwrap(),
                },
            )
            .unwrap();
        let canonical = repairer.path_for_digest(&record.digest);
        fs::write(&canonical, b"corrupt").unwrap();
        File::options()
            .write(true)
            .open(&canonical)
            .unwrap()
            .set_times(FileTimes::new().set_modified(SystemTime::now() - Duration::from_secs(7200)))
            .unwrap();
        let (mut staging, staging_path, _active) = repairer.create_staging_file().unwrap();
        staging.write_all(b"repair race").unwrap();
        staging.sync_all().unwrap();
        drop(staging);

        let (visible_tx, visible_rx) = mpsc::channel();
        let (collecting_tx, collecting_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let collect = thread::spawn(move || {
            visible_rx.recv().unwrap();
            collecting_tx.send(()).unwrap();
            let result =
                collector.collect_unreferenced(&BTreeSet::new(), Duration::from_secs(3600));
            done_tx.send(result).unwrap();
        });
        let digest_lock = repairer.state.get(&record.digest);
        let guard = digest_lock.lock().unwrap();

        repairer
            .replace_corrupt_blob(&record.digest, &staging_path, &canonical, |quarantine| {
                visible_tx.send(()).unwrap();
                collecting_rx.recv().unwrap();
                assert!(
                    done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
                    "quarantine GC did not wait for the digest lock"
                );
                assert!(quarantine.exists());
            })
            .unwrap();
        drop(guard);

        assert_eq!(done_rx.recv().unwrap().unwrap(), 0);
        collect.join().unwrap();
        repairer.verify(&record.digest).unwrap();
        assert_eq!(fs::read(canonical).unwrap(), b"repair race");
        assert_eq!(fs::read_dir(&repairer.quarantine_root).unwrap().count(), 1);
    }
}
