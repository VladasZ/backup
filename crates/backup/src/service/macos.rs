use std::env;
use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};
use directories::BaseDirs;
use tracing::warn;

use super::shared::{remove_if_present, run_command, write_atomic};

const LABEL: &str = "com.vladas.backup";

pub fn install(config: &Path) -> Result<()> {
    let executable = env::current_exe().context("find current backup executable")?;
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
        xml_escape(&executable),
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

pub fn uninstall() -> Result<()> {
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

fn user_id() -> Result<String> {
    let output = Command::new("id").arg("-u").output()?;
    if !output.status.success() {
        bail!("id -u failed");
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn xml_escape(path: &Path) -> String {
    path.display()
        .to_string()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
