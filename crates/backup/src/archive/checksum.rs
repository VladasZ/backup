use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use blake3::Hasher;
use uuid::Uuid;

pub fn checksum_file(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Hasher::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .with_context(|| format!("hash {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

pub fn write_checksum(archive: &Path, checksum: &str) -> Result<PathBuf> {
    let file_name = archive
        .file_name()
        .context("archive path has no file name")?
        .to_string_lossy();
    let checksum_path = PathBuf::from(format!("{}.blake3", archive.display()));
    let partial_path = PathBuf::from(format!(
        "{}.{}.partial",
        checksum_path.display(),
        Uuid::new_v4()
    ));
    let mut file = File::create(&partial_path)
        .with_context(|| format!("create {}", partial_path.display()))?;
    writeln!(file, "{checksum}  {file_name}")?;
    file.sync_all()?;
    fs::rename(&partial_path, &checksum_path).with_context(|| {
        format!(
            "publish checksum {} as {}",
            partial_path.display(),
            checksum_path.display()
        )
    })?;
    Ok(checksum_path)
}

pub fn read_checksum(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut line = String::new();
    BufReader::new(file).read_line(&mut line)?;
    line.split_whitespace()
        .next()
        .map(str::to_owned)
        .context("checksum file is empty")
}

pub fn verify_checksum(archive: &Path, expected: &str) -> Result<()> {
    let actual = checksum_file(archive)?;
    if actual != expected {
        bail!(
            "checksum mismatch for {}: expected {expected}, got {actual}",
            archive.display()
        );
    }
    Ok(())
}
