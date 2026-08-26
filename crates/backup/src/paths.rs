#[cfg(target_os = "linux")]
use std::env;
use std::fs;
#[cfg(target_os = "linux")]
use std::path::Path;
use std::path::PathBuf;

use anyhow::{Context, Result};
use directories::{BaseDirs, ProjectDirs};

#[derive(Clone, Debug)]
pub struct AppPaths {
    pub config: PathBuf,
    pub state: PathBuf,
    pub database: PathBuf,
    pub daemon_lock: PathBuf,
    pub operation_lock: PathBuf,
    pub staging: PathBuf,
    pub log_directory: PathBuf,
    pub log_file: PathBuf,
}

impl AppPaths {
    pub fn discover(config_override: Option<PathBuf>) -> Result<Self> {
        let base = BaseDirs::new().context("cannot determine the user home directory")?;
        let project = ProjectDirs::from("", "", "backup")
            .context("cannot determine application directories")?;
        let config = config_override.unwrap_or_else(|| project.config_dir().join("config.toml"));
        #[cfg(target_os = "linux")]
        let state = linux_state_directory(base.home_dir());
        #[cfg(not(target_os = "linux"))]
        let state = project.data_local_dir().to_path_buf();
        #[cfg(target_os = "macos")]
        let log_directory = base.home_dir().join("Library/Logs/backup");
        #[cfg(not(target_os = "macos"))]
        let log_directory = state.join("logs");
        Ok(Self {
            config,
            database: state.join("state.redb"),
            daemon_lock: state.join("daemon.lock"),
            operation_lock: state.join("operation.lock"),
            staging: state.join("staging"),
            log_file: log_directory.join("backup.log"),
            log_directory,
            state,
        })
    }

    pub fn ensure(&self) -> Result<()> {
        for directory in [&self.state, &self.staging, &self.log_directory] {
            fs::create_dir_all(directory)
                .with_context(|| format!("create directory {}", directory.display()))?;
        }
        Ok(())
    }

    pub fn job_staging(&self, job: &str) -> PathBuf {
        self.staging.join(job)
    }
}

#[cfg(target_os = "linux")]
fn linux_state_directory(home: &Path) -> PathBuf {
    env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".local/state"))
        .join("backup")
}
