use std::{fmt, fs, io, path::Path, path::PathBuf};

use directories::ProjectDirs;
use thiserror::Error;

/// The only source of YakShed-owned filesystem locations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppPaths {
    pub config_root: PathBuf,
    pub cache_root: PathBuf,
    pub data_root: PathBuf,
    pub runtime_root: PathBuf,
}

impl AppPaths {
    /// Resolves platform-native production locations without creating them.
    pub fn production() -> Result<Self, PathError> {
        // Keep this durable identity aligned with the keyring service
        // `dev.yakshed.YakShed` specified by working-with-secrets.md §9.1.
        let project = ProjectDirs::from("dev", "yakshed", "YakShed")
            .ok_or(PathError::PlatformDirectoriesUnavailable)?;

        #[cfg(target_os = "linux")]
        let paths = Self {
            config_root: project.config_dir().to_owned(),
            cache_root: project.cache_dir().to_owned(),
            data_root: project
                .state_dir()
                .unwrap_or_else(|| project.data_local_dir())
                .to_owned(),
            runtime_root: project
                .runtime_dir()
                .map_or_else(|| project.data_local_dir().join("runtime"), Path::to_owned),
        };

        #[cfg(target_os = "macos")]
        let paths = {
            let project_root = project.data_dir();
            let data_root = project_root.join("data");
            Self {
                config_root: project_root.join("config"),
                cache_root: project.cache_dir().to_owned(),
                runtime_root: data_root.join("runtime"),
                data_root,
            }
        };

        #[cfg(target_os = "windows")]
        let paths = {
            let data_root = project.data_local_dir().to_owned();
            Self {
                config_root: project.config_dir().to_owned(),
                cache_root: project.cache_dir().to_owned(),
                runtime_root: data_root.join("runtime"),
                data_root,
            }
        };

        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        compile_error!("YakShed AppPaths policy is not defined for this target OS");

        Ok(paths)
    }

    /// Creates isolated paths entirely beneath `root`.
    pub fn for_test(root: &Path) -> Self {
        Self {
            config_root: root.join("config"),
            cache_root: root.join("cache"),
            data_root: root.join("data"),
            runtime_root: root.join("runtime"),
        }
    }

    /// Returns all state roots in config, cache, data, runtime order.
    pub fn roots(&self) -> [&Path; 4] {
        [
            &self.config_root,
            &self.cache_root,
            &self.data_root,
            &self.runtime_root,
        ]
    }

    /// Creates every root with private POSIX permissions.
    pub fn create_dirs(&self) -> Result<(), PathError> {
        for path in self.roots() {
            fs::create_dir_all(path).map_err(|source| PathError::Io {
                path: path.to_owned(),
                source,
            })?;
            set_private_directory_permissions(path)?;
        }
        Ok(())
    }

    /// Formats the resolved roots for diagnostics and support output.
    pub fn diagnostics(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for AppPaths {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "config={} cache={} data={} runtime={}",
            self.config_root.display(),
            self.cache_root.display(),
            self.data_root.display(),
            self.runtime_root.display()
        )
    }
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<(), PathError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|source| PathError::Io {
        path: path.to_owned(),
        source,
    })
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<(), PathError> {
    Ok(())
}

/// Failure to resolve or create application paths.
#[derive(Debug, Error)]
pub enum PathError {
    #[error("platform application directories are unavailable")]
    PlatformDirectoriesUnavailable,
    #[error("filesystem operation failed for {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}
