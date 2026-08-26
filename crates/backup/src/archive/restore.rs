use std::fs::File;
use std::io::{Read, Write, copy, sink};
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};
use lz4_flex::frame::FrameDecoder;
use tar::Archive;

pub fn restore_archive(archive: &Path, target: &Path) -> Result<()> {
    // Only root can change a file's owner. As a normal user the chown always fails,
    // so restoring ownership would only turn a working restore into a failure.
    let preserve_ownerships = running_as_root();
    with_tar_reader(archive, |reader| {
        let mut tar = Archive::new(reader);
        tar.set_overwrite(true);
        tar.set_preserve_ownerships(preserve_ownerships);
        tar.set_preserve_permissions(true);
        tar.set_preserve_mtime(true);
        tar.set_unpack_xattrs(true);
        tar.unpack(target)
            .with_context(|| format!("restore into {}", target.display()))
    })
}

fn running_as_root() -> bool {
    Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .is_some_and(|id| id.trim() == "0")
}

pub fn verify_archive(archive: &Path) -> Result<()> {
    with_tar_reader(archive, |reader| {
        let mut tar = Archive::new(reader);
        for entry in tar.entries().context("read TAR entries")? {
            let mut entry = entry.context("read TAR entry")?;
            copy(&mut entry, &mut sink()).context("verify TAR entry contents")?;
        }
        Ok(())
    })
}

fn with_tar_reader<T>(
    archive: &Path,
    operation: impl FnOnce(&mut dyn Read) -> Result<T>,
) -> Result<T> {
    let file = File::open(archive).with_context(|| format!("open {}", archive.display()))?;
    let mut reader = FrameDecoder::new(file);
    operation(&mut reader)
}

pub fn copy_archive_contents(archive: &Path, writer: &mut dyn Write) -> Result<()> {
    let mut file = File::open(archive).with_context(|| format!("open {}", archive.display()))?;
    copy(&mut file, writer).with_context(|| format!("read {}", archive.display()))?;
    Ok(())
}
