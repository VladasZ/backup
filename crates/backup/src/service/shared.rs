use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};

pub fn run_command(command: &mut Command, description: &str) -> Result<()> {
    let status = command
        .status()
        .with_context(|| format!("{description}: start command"))?;
    if !status.success() {
        bail!("{description}: command exited with {status}");
    }
    Ok(())
}

pub fn write_atomic(path: &Path, contents: &[u8]) -> Result<()> {
    let partial = PathBuf::from(format!("{}.partial", path.display()));
    remove_if_present(&partial)?;
    let mut file = File::create(&partial)?;
    file.write_all(contents)?;
    file.sync_all()?;
    fs::rename(&partial, path)?;
    Ok(())
}

pub fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}
