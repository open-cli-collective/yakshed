use std::{
    collections::{BTreeSet, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

use sha2::{Digest, Sha256};
use thiserror::Error;
use yakshed_domain::{
    ArtifactId, ArtifactKind, ArtifactProvenance, ArtifactRecord, ContentDigest, RunId, WorkItemId,
};

use crate::AppPaths;

static NEXT_STAGING_FILE: AtomicU64 = AtomicU64::new(0);

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
pub struct ArtifactStore<C = SystemClock> {
    sha256_root: PathBuf,
    staging_root: PathBuf,
    max_size: u64,
    clock: C,
}

impl ArtifactStore {
    pub fn new(paths: &AppPaths, max_size: u64) -> Result<Self, ArtifactError> {
        Self::with_clock(paths, max_size, SystemClock)
    }
}

impl<C: Clock> ArtifactStore<C> {
    pub fn with_clock(paths: &AppPaths, max_size: u64, clock: C) -> Result<Self, ArtifactError> {
        let artifacts_root = paths.data_root.join("artifacts");
        let sha256_root = artifacts_root.join("sha256");
        let staging_root = artifacts_root.join("staging");
        for directory in [
            &paths.data_root,
            &artifacts_root,
            &sha256_root,
            &staging_root,
        ] {
            create_private_directory(directory)?;
        }
        Ok(Self {
            sha256_root,
            staging_root,
            max_size,
            clock,
        })
    }

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
        create_private_directory(destination.parent().expect("digest path has a parent"))?;

        if destination.try_exists()? {
            self.verify(&digest)?;
            fs::remove_file(&staging_path)?;
        } else {
            match fs::rename(&staging_path, &destination) {
                Ok(()) => set_private_file_permissions(&destination)?,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    self.verify(&digest)?;
                    fs::remove_file(&staging_path)?;
                }
                Err(error) => return Err(ArtifactError::Io(error)),
            }
        }

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

    pub fn collect_unreferenced(
        &self,
        referenced: &BTreeSet<ContentDigest>,
        grace: Duration,
    ) -> Result<usize, ArtifactError> {
        let now = self.clock.now();
        let mut removed = collect_old_files(&self.staging_root, now, grace, &HashSet::new())?;
        let referenced: HashSet<_> = referenced.iter().map(ContentDigest::as_str).collect();

        for shard in fs::read_dir(&self.sha256_root)? {
            let shard = shard?;
            if shard.file_type()?.is_dir() {
                removed += collect_old_files(&shard.path(), now, grace, &referenced)?;
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

fn collect_old_files(
    root: &Path,
    now: SystemTime,
    grace: Duration,
    referenced: &HashSet<&str>,
) -> Result<usize, ArtifactError> {
    let mut removed = 0;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        if referenced.contains(name.to_string_lossy().as_ref()) {
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

fn create_private_directory(path: &Path) -> Result<(), ArtifactError> {
    fs::create_dir_all(path)?;
    set_private_directory_permissions(path)
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
