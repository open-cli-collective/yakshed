//! Storage infrastructure for application paths and non-secret configuration.

mod config;
mod paths;

pub use config::{ConfigError, ConfigStore};
pub use paths::{AppPaths, PathError};
