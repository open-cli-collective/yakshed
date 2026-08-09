use std::collections::HashSet;

use tempfile::tempdir;
use yakshed_store::AppPaths;

#[test]
fn test_paths_are_distinct_and_contained() {
    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    let roots = [
        &paths.config_root,
        &paths.cache_root,
        &paths.data_root,
        &paths.runtime_root,
    ];

    assert!(roots.iter().all(|path| path.starts_with(temp.path())));
    assert_eq!(roots.into_iter().collect::<HashSet<_>>().len(), 4);
}

#[cfg(unix)]
#[test]
fn created_directories_are_private() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    paths.create_dirs().unwrap();

    for path in paths.roots() {
        assert_eq!(path.metadata().unwrap().permissions().mode() & 0o777, 0o700);
    }
}

#[test]
fn production_paths_follow_current_platform_policy_without_creating_them() {
    use directories::ProjectDirs;

    let paths = AppPaths::production().unwrap();
    let project = ProjectDirs::from("dev", "yakshed", "YakShed").unwrap();
    assert!(paths.roots().iter().all(|path| path.is_absolute()));
    assert_eq!(paths.roots().into_iter().collect::<HashSet<_>>().len(), 4);

    #[cfg(target_os = "macos")]
    {
        let root = project.data_dir();
        assert_eq!(paths.config_root, root.join("config"));
        assert_eq!(paths.cache_root, project.cache_dir());
        assert_eq!(paths.data_root, root.join("data"));
        assert_eq!(paths.runtime_root, root.join("data/runtime"));
    }

    #[cfg(target_os = "windows")]
    {
        assert_eq!(paths.config_root, project.config_dir());
        assert_eq!(paths.cache_root, project.cache_dir());
        assert_eq!(paths.data_root, project.data_local_dir());
        assert_eq!(paths.runtime_root, project.data_local_dir().join("runtime"));
    }

    #[cfg(target_os = "linux")]
    {
        assert_eq!(paths.config_root, project.config_dir());
        assert_eq!(paths.cache_root, project.cache_dir());
        assert_eq!(paths.data_root, project.state_dir().unwrap());
        assert_eq!(
            paths.runtime_root,
            project
                .runtime_dir()
                .map_or_else(|| project.data_local_dir().join("runtime"), Into::into)
        );
    }
}

#[test]
fn diagnostics_include_every_resolved_root() {
    let temp = tempdir().unwrap();
    let paths = AppPaths::for_test(temp.path());
    let display = paths.diagnostics();

    for path in paths.roots() {
        assert!(display.contains(path.to_str().unwrap()));
    }
}
