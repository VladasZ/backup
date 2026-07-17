use std::env;
use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use directories::BaseDirs;
use tracing::warn;

const LABEL: &str = "com.vladas.backup";

pub fn install(config: &Path) -> Result<()> {
    let executable = env::current_exe().context("find current backup executable")?;
    #[cfg(target_os = "macos")]
    install_macos(&executable, config)?;
    #[cfg(target_os = "linux")]
    install_linux(&executable, config)?;
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    bail!("service installation is supported only on macOS and Linux");
    Ok(())
}

pub fn uninstall() -> Result<()> {
    #[cfg(target_os = "macos")]
    uninstall_macos()?;
    #[cfg(target_os = "linux")]
    uninstall_linux()?;
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    bail!("service installation is supported only on macOS and Linux");
    Ok(())
}

#[cfg(target_os = "macos")]
fn install_macos(executable: &Path, config: &Path) -> Result<()> {
    let base = BaseDirs::new().context("cannot determine home directory")?;
    let directory = base.home_dir().join("Library/LaunchAgents");
    fs::create_dir_all(&directory)?;
    let service = directory.join(format!("{LABEL}.plist"));
    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{}</string>
    <string>--config</string>
    <string>{}</string>
    <string>daemon</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ProcessType</key>
  <string>Background</string>
  <key>ExitTimeOut</key>
  <integer>86400</integer>
</dict>
</plist>
"#,
        xml_escape(executable),
        xml_escape(config)
    );
    write_atomic(&service, plist.as_bytes())?;
    let domain = format!("gui/{}", user_id()?);
    let bootout = Command::new("launchctl")
        .args(["bootout", &domain])
        .arg(&service)
        .status()
        .context("stop existing launch agent")?;
    if !bootout.success() {
        warn!("launch agent was not already loaded");
    }
    run_command(
        Command::new("launchctl")
            .args(["bootstrap", &domain])
            .arg(&service),
        "load launch agent",
    )?;
    println!("installed and started {LABEL}");
    Ok(())
}

#[cfg(target_os = "macos")]
fn uninstall_macos() -> Result<()> {
    let base = BaseDirs::new().context("cannot determine home directory")?;
    let service = base
        .home_dir()
        .join("Library/LaunchAgents")
        .join(format!("{LABEL}.plist"));
    let domain = format!("gui/{}", user_id()?);
    let status = Command::new("launchctl")
        .args(["bootout", &domain])
        .arg(&service)
        .status()
        .context("stop launch agent")?;
    if !status.success() {
        warn!("launch agent was not loaded");
    }
    remove_if_present(&service)?;
    println!("uninstalled {LABEL}");
    Ok(())
}

#[cfg(target_os = "macos")]
fn user_id() -> Result<String> {
    let output = Command::new("id").arg("-u").output()?;
    if !output.status.success() {
        bail!("id -u failed");
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

#[cfg(target_os = "macos")]
fn xml_escape(path: &Path) -> String {
    path.display()
        .to_string()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(target_os = "linux")]
fn install_linux(executable: &Path, config: &Path) -> Result<()> {
    let base = BaseDirs::new().context("cannot determine home directory")?;
    let directory = base.home_dir().join(".config/systemd/user");
    fs::create_dir_all(&directory)?;
    let service = directory.join("backup.service");
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
        systemd_quote(executable),
        systemd_quote(config)
    );
    write_atomic(&service, unit.as_bytes())?;
    run_command(
        Command::new("systemctl").args(["--user", "daemon-reload"]),
        "reload user services",
    )?;
    run_command(
        Command::new("systemctl").args(["--user", "enable", "--now", "backup.service"]),
        "enable backup service",
    )?;
    println!("installed and started backup.service");
    Ok(())
}

#[cfg(target_os = "linux")]
fn uninstall_linux() -> Result<()> {
    let base = BaseDirs::new().context("cannot determine home directory")?;
    let service = base.home_dir().join(".config/systemd/user/backup.service");
    let status = Command::new("systemctl")
        .args(["--user", "disable", "--now", "backup.service"])
        .status()
        .context("disable backup service")?;
    if !status.success() {
        warn!("backup.service was not loaded");
    }
    remove_if_present(&service)?;
    run_command(
        Command::new("systemctl").args(["--user", "daemon-reload"]),
        "reload user services",
    )?;
    println!("uninstalled backup.service");
    Ok(())
}

#[cfg(target_os = "linux")]
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

fn run_command(command: &mut Command, description: &str) -> Result<()> {
    let status = command
        .status()
        .with_context(|| format!("{description}: start command"))?;
    if !status.success() {
        bail!("{description}: command exited with {status}");
    }
    Ok(())
}

fn write_atomic(path: &Path, contents: &[u8]) -> Result<()> {
    let partial = PathBuf::from(format!("{}.partial", path.display()));
    remove_if_present(&partial)?;
    let mut file = File::create(&partial)?;
    file.write_all(contents)?;
    file.sync_all()?;
    fs::rename(&partial, path)?;
    Ok(())
}

fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}
