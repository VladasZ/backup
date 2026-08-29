use std::process::Command;

use anyhow::{Context, Result, bail};
use tracing::info;

// Output is captured rather than inherited because the remote agent writes its
// protocol frames to stdout, and a pre command that printed there would corrupt
// the stream the controller is reading.
pub fn run(job: &str, command: &str) -> Result<()> {
    info!(job, command, "running the pre command");
    let output = Command::new("sh")
        .arg("-c")
        .arg(command)
        .output()
        .with_context(|| format!("start the pre command for job {job:?}"))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let detail = stderr.trim();
    if detail.is_empty() {
        bail!("pre command for job {job:?} failed with {}", output.status);
    }
    bail!(
        "pre command for job {job:?} failed with {}: {detail}",
        output.status
    );
}
