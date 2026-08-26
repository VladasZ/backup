use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};
use directories::BaseDirs;
use tracing::warn;

use super::shared::{remove_if_present, run_command, write_atomic};

const UNIT: &str = "backup.service";

pub fn install(config: &Path) -> Result<()> {
    let executable = env::current_exe().context("find current backup executable")?;
    let base = BaseDirs::new().context("cannot determine home directory")?;
    let directory = base.home_dir().join(".config/systemd/user");
    fs::create_dir_all(&directory)?;
    let service = directory.join(UNIT);
    let unit = format!(
        r#"[Unit]
Description=Backup service
After=network-online.target

[Service]
Type=simple
ExecStart={} --config {} daemon
Restart=on-failure
RestartSec=10
TimeoutStopSec=24h

[Install]
WantedBy=default.target
"#,
        systemd_quote(&executable),
        systemd_quote(config)
    );
    write_atomic(&service, unit.as_bytes())?;
    run_command(
        Command::new("systemctl").args(["--user", "daemon-reload"]),
        "reload user services",
    )?;
    run_command(
        Command::new("systemctl").args(["--user", "enable", "--now", UNIT]),
        "enable backup service",
    )?;
    println!("installed and started {UNIT}");
    Ok(())
}

pub fn uninstall() -> Result<()> {
    let base = BaseDirs::new().context("cannot determine home directory")?;
    let service = base.home_dir().join(".config/systemd/user").join(UNIT);
    let status = Command::new("systemctl")
        .args(["--user", "disable", "--now", UNIT])
        .status()
        .context("disable backup service")?;
    if !status.success() {
        warn!("{UNIT} was not loaded");
    }
    remove_if_present(&service)?;
    run_command(
        Command::new("systemctl").args(["--user", "daemon-reload"]),
        "reload user services",
    )?;
    println!("uninstalled {UNIT}");
    Ok(())
}

fn systemd_quote(path: &Path) -> String {
    format!(
        "\"{}\"",
        path.display()
            .to_string()
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
    )
}
