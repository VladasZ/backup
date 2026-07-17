use std::fs::File;
use std::io::{Read, Write, copy, sink};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::thread;

use anyhow::{Context, Error, Result, anyhow, bail};
use flate2::read::GzDecoder;
use tar::Archive;
use zpaq_rs::decompress_stream;
use zstd::stream::read::Decoder as ZstdDecoder;

pub fn restore_archive(archive: &Path, target: &Path) -> Result<()> {
    with_tar_reader(archive, |reader| {
        let mut tar = Archive::new(reader);
        tar.set_overwrite(true);
        tar.set_preserve_ownerships(true);
        tar.set_preserve_permissions(true);
        tar.set_preserve_mtime(true);
        tar.set_unpack_xattrs(true);
        tar.unpack(target)
            .with_context(|| format!("restore into {}", target.display()))
    })
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
    let name = archive
        .file_name()
        .context("archive path has no file name")?
        .to_string_lossy();
    let file = File::open(archive).with_context(|| format!("open {}", archive.display()))?;
    if name.ends_with(".tar") {
        let mut reader = file;
        return operation(&mut reader);
    }
    if name.ends_with(".tar.gz") {
        let mut reader = GzDecoder::new(file);
        return operation(&mut reader);
    }
    if name.ends_with(".tar.zst") {
        let mut reader = ZstdDecoder::new(file)?;
        return operation(&mut reader);
    }
    if name.ends_with(".tar.zpaq") {
        return with_zpaq_reader(file, operation);
    }
    bail!("unsupported archive extension for {}", archive.display())
}

fn with_zpaq_reader<T>(
    file: File,
    operation: impl FnOnce(&mut dyn Read) -> Result<T>,
) -> Result<T> {
    let (mut reader, writer) = UnixStream::pair().context("create ZPAQ decompression pipe")?;
    let decompressor =
        thread::spawn(move || decompress_stream(file, writer).map_err(|error| error.to_string()));
    let operation_result = operation(&mut reader);
    let drain_result = copy(&mut reader, &mut sink());
    let decompression_result = decompressor
        .join()
        .map_err(|_| anyhow!("ZPAQ decompressor thread panicked"))?;
    let value = operation_result?;
    drain_result.context("drain ZPAQ stream")?;
    decompression_result.map_err(Error::msg)?;
    Ok(value)
}

pub fn copy_archive_contents(archive: &Path, writer: &mut dyn Write) -> Result<()> {
    let mut file = File::open(archive).with_context(|| format!("open {}", archive.display()))?;
    copy(&mut file, writer).with_context(|| format!("read {}", archive.display()))?;
    Ok(())
}
