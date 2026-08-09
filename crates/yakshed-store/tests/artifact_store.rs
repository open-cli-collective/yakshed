use std::{
    collections::BTreeSet,
    fs::{self, File, FileTimes},
    io::{self, Read},
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
    thread,
    time::{Duration, SystemTime},
};

use tempfile::TempDir;
use yakshed_domain::{
    ArtifactId, ArtifactKind, ArtifactProvenance, ContentDigest, RunId, WorkItemId,
};
use yakshed_store::{AppPaths, ArtifactError, ArtifactMetadata, ArtifactStore, Clock};

struct TestClock(SystemTime);

impl Clock for TestClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

fn fixture(max_size: u64) -> (TempDir, AppPaths, ArtifactStore) {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_data_root().unwrap();
    let store = ArtifactStore::new(&paths, max_size).unwrap();
    (temp, paths, store)
}

fn metadata(id: &str) -> ArtifactMetadata {
    ArtifactMetadata {
        id: id.parse::<ArtifactId>().unwrap(),
        work_item_id: "0193f26e-7a72-7d42-bf77-0de14c4cc221"
            .parse::<WorkItemId>()
            .unwrap(),
        run_id: Some(
            "0193f26e-7a72-7d42-bf77-0de14c4cc222"
                .parse::<RunId>()
                .unwrap(),
        ),
        kind: ArtifactKind::Plan,
        media_type: "text/plain".to_owned(),
        provenance: ArtifactProvenance::new("provider:codex").unwrap(),
    }
}

fn blob_path(paths: &AppPaths, digest: &ContentDigest) -> PathBuf {
    paths
        .data_root
        .join("artifacts/sha256")
        .join(&digest.as_str()[..2])
        .join(digest.as_str())
}

fn file_count(root: &Path) -> usize {
    fs::read_dir(root)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| {
            if entry.file_type().unwrap().is_dir() {
                file_count(&entry.path())
            } else {
                1
            }
        })
        .sum()
}

#[test]
fn digest_matches_known_vector_and_layout() {
    let (_temp, paths, store) = fixture(1024);
    let record = store
        .publish(
            &b"abc"[..],
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc220"),
        )
        .unwrap();

    assert_eq!(
        record.digest.as_str(),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(record.byte_len, 3);
    assert_eq!(
        blob_path(&paths, &record.digest),
        paths
            .data_root
            .join("artifacts/sha256/ba")
            .join("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
    );
    assert_eq!(fs::read(blob_path(&paths, &record.digest)).unwrap(), b"abc");
}

#[test]
fn identical_content_is_deduplicated() {
    let (_temp, paths, store) = fixture(1024);
    let first = store
        .publish(
            &b"same"[..],
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc223"),
        )
        .unwrap();
    let second = store
        .publish(
            &b"same"[..],
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc224"),
        )
        .unwrap();

    assert_eq!(first.digest, second.digest);
    assert_eq!(file_count(&paths.data_root.join("artifacts/sha256")), 1);
}

#[test]
fn dedup_reuse_refreshes_mtime() {
    let (_temp, paths, store) = fixture(1024);
    let first = store
        .publish(
            &b"leased"[..],
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc233"),
        )
        .unwrap();
    let path = blob_path(&paths, &first.digest);
    let old = SystemTime::now() - Duration::from_secs(7200);
    File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(FileTimes::new().set_modified(old))
        .unwrap();

    store
        .publish(
            &b"leased"[..],
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc234"),
        )
        .unwrap();

    assert!(fs::metadata(path).unwrap().modified().unwrap() > old);
}

#[test]
fn republishing_over_corrupt_blob_quarantines_and_repairs() {
    let (_temp, paths, store) = fixture(1024);
    let original = store
        .publish(
            &b"repairable"[..],
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc235"),
        )
        .unwrap();
    let canonical = blob_path(&paths, &original.digest);
    fs::write(&canonical, b"corrupted!").unwrap();
    let references = BTreeSet::from([original.digest.clone()]);
    assert_eq!(
        store
            .collect_unreferenced(&references, Duration::ZERO)
            .unwrap(),
        0
    );

    let repaired = store
        .publish(
            &b"repairable"[..],
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc236"),
        )
        .unwrap();

    assert_eq!(repaired.digest, original.digest);
    assert_eq!(fs::read(&canonical).unwrap(), b"repairable");
    store.verify(&repaired.digest).unwrap();
    let quarantined: Vec<_> = fs::read_dir(paths.data_root.join("artifacts/quarantine"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(quarantined.len(), 1);
    assert_eq!(fs::read(&quarantined[0]).unwrap(), b"corrupted!");
    assert!(
        quarantined[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(original.digest.as_str())
    );
}

#[test]
fn concurrent_republish_across_store_handles_survives_collection() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_data_root().unwrap();
    let publisher_store = Arc::new(ArtifactStore::new(&paths, 1024).unwrap());
    let collector_store = Arc::new(ArtifactStore::new(&paths, 1024).unwrap());
    let record = publisher_store
        .publish(
            &b"raced"[..],
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc237"),
        )
        .unwrap();
    let path = blob_path(&paths, &record.digest);

    for _ in 0..64 {
        let old = SystemTime::now() - Duration::from_secs(7200);
        File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(FileTimes::new().set_modified(old))
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let publisher = Arc::clone(&publisher_store);
        let publish_barrier = Arc::clone(&barrier);
        let publish = thread::spawn(move || {
            publish_barrier.wait();
            publisher.publish(
                &b"raced"[..],
                metadata("0193f26e-7a72-7d42-bf77-0de14c4cc238"),
            )
        });
        let collector = Arc::clone(&collector_store);
        let collect = thread::spawn(move || {
            barrier.wait();
            collector.collect_unreferenced(&BTreeSet::new(), Duration::from_secs(3600))
        });

        publish.join().unwrap().unwrap();
        collect.join().unwrap().unwrap();
        assert!(path.exists());
        publisher_store.verify(&record.digest).unwrap();
    }
}

#[test]
fn quarantine_collection_respects_grace_period() {
    let (_temp, paths, store) = fixture(1024);
    let quarantine = paths.data_root.join("artifacts/quarantine");
    let old = quarantine.join("old-corrupt-blob");
    let young = quarantine.join("young-corrupt-blob");
    fs::write(&old, b"old sensitive debris").unwrap();
    fs::write(&young, b"young sensitive debris").unwrap();
    File::options()
        .write(true)
        .open(&old)
        .unwrap()
        .set_times(FileTimes::new().set_modified(SystemTime::now() - Duration::from_secs(7200)))
        .unwrap();

    assert_eq!(
        store
            .collect_unreferenced(&BTreeSet::new(), Duration::from_secs(3600))
            .unwrap(),
        1
    );
    assert!(!old.exists());
    assert!(young.exists());
}

#[test]
fn maximum_size_is_enforced_and_staging_is_cleaned() {
    let (_temp, paths, store) = fixture(3);
    let error = store
        .publish(
            &b"abcd"[..],
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc225"),
        )
        .unwrap_err();

    assert!(matches!(error, ArtifactError::TooLarge { max_size: 3 }));
    let digest = "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589"
        .parse::<ContentDigest>()
        .unwrap();
    assert!(!blob_path(&paths, &digest).exists());
    assert_eq!(file_count(&paths.data_root.join("artifacts/staging")), 0);
}

struct FailingReader(bool);

impl Read for FailingReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.0 {
            Err(io::Error::other("simulated interruption"))
        } else {
            self.0 = true;
            buffer[..7].copy_from_slice(b"partial");
            Ok(7)
        }
    }
}

#[test]
fn interrupted_publish_leaves_only_collectable_staging() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_data_root().unwrap();
    let store = ArtifactStore::with_clock(&paths, 1024, TestClock(SystemTime::now())).unwrap();

    assert!(matches!(
        store.publish(
            FailingReader(false),
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc226")
        ),
        Err(ArtifactError::Io(_))
    ));
    assert_eq!(file_count(&paths.data_root.join("artifacts/sha256")), 0);
    assert_eq!(file_count(&paths.data_root.join("artifacts/staging")), 1);
    assert_eq!(
        store
            .collect_unreferenced(&BTreeSet::new(), Duration::from_secs(3600))
            .unwrap(),
        0
    );

    let collector = ArtifactStore::with_clock(
        &paths,
        1024,
        TestClock(SystemTime::now() + Duration::from_secs(7200)),
    )
    .unwrap();
    assert_eq!(
        collector
            .collect_unreferenced(&BTreeSet::new(), Duration::from_secs(3600))
            .unwrap(),
        1
    );
    assert_eq!(file_count(&paths.data_root.join("artifacts/staging")), 0);
}

#[test]
fn garbage_collection_respects_references_and_grace_period() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_data_root().unwrap();
    let now = SystemTime::now();
    let store = ArtifactStore::with_clock(&paths, 1024, TestClock(now)).unwrap();
    let old = store
        .publish(
            &b"old"[..],
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc227"),
        )
        .unwrap();
    let referenced = store
        .publish(
            &b"referenced"[..],
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc228"),
        )
        .unwrap();
    let young = store
        .publish(
            &b"young"[..],
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc229"),
        )
        .unwrap();
    let old_time = now - Duration::from_secs(7200);
    for record in [&old, &referenced] {
        File::options()
            .write(true)
            .open(blob_path(&paths, &record.digest))
            .unwrap()
            .set_times(FileTimes::new().set_modified(old_time))
            .unwrap();
    }
    let references = BTreeSet::from([referenced.digest.clone()]);

    assert_eq!(
        store
            .collect_unreferenced(&references, Duration::from_secs(3600))
            .unwrap(),
        1
    );
    assert!(!blob_path(&paths, &old.digest).exists());
    assert!(blob_path(&paths, &referenced.digest).exists());
    assert!(blob_path(&paths, &young.digest).exists());
}

#[test]
fn digest_validation_makes_traversal_unrepresentable() {
    for invalid in [
        "../../etc/passwd",
        "BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD",
        "ba7816bf",
        "za7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    ] {
        assert!(invalid.parse::<ContentDigest>().is_err(), "{invalid}");
    }
}

#[test]
fn invalid_metadata_and_missing_blobs_return_typed_errors() {
    let (_temp, paths, store) = fixture(1024);
    let mut invalid = metadata("0193f26e-7a72-7d42-bf77-0de14c4cc232");
    invalid.media_type.clear();
    assert!(matches!(
        store.publish(&b"content"[..], invalid),
        Err(ArtifactError::InvalidInput(_))
    ));
    assert_eq!(file_count(&paths.data_root.join("artifacts/staging")), 0);

    let missing = "0000000000000000000000000000000000000000000000000000000000000000"
        .parse::<ContentDigest>()
        .unwrap();
    assert!(matches!(
        store.open(&missing, 1),
        Err(ArtifactError::NotFound(digest)) if digest == missing
    ));
}

#[test]
fn open_is_bounded_and_verify_detects_corruption() {
    let (_temp, paths, store) = fixture(1024);
    let record = store
        .publish(
            &b"abcdef"[..],
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc230"),
        )
        .unwrap();

    assert!(matches!(
        store.open(&record.digest, 3),
        Err(ArtifactError::BoundExceeded {
            byte_len: 6,
            max_bytes: 3
        })
    ));
    let mut reader = store.open(&record.digest, record.byte_len).unwrap();
    let mut content = Vec::new();
    reader.read_to_end(&mut content).unwrap();
    assert_eq!(content, b"abcdef");

    fs::write(blob_path(&paths, &record.digest), b"abcdeg").unwrap();
    assert!(matches!(
        store.verify(&record.digest),
        Err(ArtifactError::DigestMismatch { .. })
    ));
}

#[cfg(unix)]
#[test]
fn artifact_files_and_directories_are_private() {
    use std::os::unix::fs::PermissionsExt;

    let (_temp, paths, store) = fixture(1024);
    let record = store
        .publish(
            &b"private"[..],
            metadata("0193f26e-7a72-7d42-bf77-0de14c4cc231"),
        )
        .unwrap();

    for directory in [
        paths.data_root.clone(),
        paths.data_root.join("artifacts"),
        paths.data_root.join("artifacts/staging"),
        paths.data_root.join("artifacts/quarantine"),
        paths.data_root.join("artifacts/sha256"),
        blob_path(&paths, &record.digest)
            .parent()
            .unwrap()
            .to_owned(),
    ] {
        assert_eq!(
            fs::metadata(directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
    assert_eq!(
        fs::metadata(blob_path(&paths, &record.digest))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}
