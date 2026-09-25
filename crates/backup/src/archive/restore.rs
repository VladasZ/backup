use std::io::{Read, copy, sink};
use std::path::Path;

use anyhow::{Context, Result};
use rustix::process::geteuid;
use tar::Archive;

pub fn restore_stream(reader: &mut dyn Read, target: &Path) -> Result<()> {
    // Only root can change a file's owner. As a normal user the chown always fails,
    // so restoring ownership would only turn a working restore into a failure.
    let preserve_ownerships = geteuid().is_root();
    {
        let mut tar = Archive::new(&mut *reader);
        tar.set_overwrite(true);
        tar.set_preserve_ownerships(preserve_ownerships);
        tar.set_preserve_permissions(true);
        tar.set_preserve_mtime(true);
        tar.set_unpack_xattrs(true);
        tar.unpack(target)
            .with_context(|| format!("restore into {}", target.display()))?;
    }
    drain(reader)
}

pub fn verify_stream(reader: &mut dyn Read) -> Result<()> {
    {
        let mut tar = Archive::new(&mut *reader);
        for entry in tar.entries().context("read TAR entries")? {
            let mut entry = entry.context("read TAR entry")?;
            copy(&mut entry, &mut sink()).context("verify TAR entry contents")?;
        }
    }
    drain(reader)
}

// The tar reader stops at the end-of-archive marker, which can leave padding
// unread. Reading to the end lets a checking stream compare the whole length.
fn drain(reader: &mut dyn Read) -> Result<()> {
    copy(reader, &mut sink()).context("read the end of the stream")?;
    Ok(())
}
