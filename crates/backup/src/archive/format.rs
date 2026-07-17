use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Write, empty};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::thread;

use anyhow::{Context, Error, Result, anyhow};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::to_vec;
use tar::{Builder, EntryType, Header, HeaderMode};
use zpaq_rs::compress_stream;
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::config::{CompressionAlgorithm, CompressionConfig};

use super::Manifest;
use super::catalog::{FileKind, FileRecord};

pub fn write_compressed_tar(
    output: &Path,
    source: &Path,
    records: &[FileRecord],
    manifest: &Manifest,
    compression: &CompressionConfig,
) -> Result<()> {
    let file = File::create(output).with_context(|| format!("create {}", output.display()))?;
    match compression.algorithm {
        CompressionAlgorithm::None => {
            let file = write_tar(file, source, records, manifest)?;
            file.sync_all()?;
        }
        CompressionAlgorithm::Gzip => {
            let encoder = GzEncoder::new(file, Compression::default());
            let encoder = write_tar(encoder, source, records, manifest)?;
            encoder.finish()?.sync_all()?;
        }
        CompressionAlgorithm::Zstd => {
            let encoder = ZstdEncoder::new(file, 0)?;
            let encoder = write_tar(encoder, source, records, manifest)?;
            encoder.finish()?.sync_all()?;
        }
        CompressionAlgorithm::Zpaq => {
            let level = compression
                .level
                .context("validated ZPAQ level is missing")?;
            write_zpaq(file, source, records, manifest, level)?;
        }
    }
    OpenOptions::new()
        .read(true)
        .open(output)?
        .sync_all()
        .with_context(|| format!("sync {}", output.display()))?;
    Ok(())
}

fn write_zpaq(
    file: File,
    source: &Path,
    records: &[FileRecord],
    manifest: &Manifest,
    level: u8,
) -> Result<()> {
    let (reader, writer) = UnixStream::pair().context("create ZPAQ compression pipe")?;
    let method = level.to_string();
    let compressor = thread::spawn(move || {
        compress_stream(reader, file, &method, Some("archive.tar"), None)
            .map_err(|error| error.to_string())
    });
    let tar_result = write_tar(writer, source, records, manifest).map(drop);
    let compression_result = compressor
        .join()
        .map_err(|_| anyhow!("ZPAQ compressor thread panicked"))?;
    tar_result?;
    compression_result.map_err(Error::msg)
}

fn write_tar<W: Write>(
    writer: W,
    source: &Path,
    records: &[FileRecord],
    manifest: &Manifest,
) -> Result<W> {
    let mut builder = Builder::new(writer);
    builder.mode(HeaderMode::Complete);
    builder.follow_symlinks(false);
    append_manifest(&mut builder, manifest)?;
    let mut hard_links = HashMap::new();
    for record in records {
        append_record(&mut builder, source, record, &mut hard_links)?;
    }
    builder.finish()?;
    builder.into_inner().context("finish TAR archive")
}

fn append_manifest<W: Write>(builder: &mut Builder<W>, manifest: &Manifest) -> Result<()> {
    let manifest_data = to_vec(manifest)?;
    let key = "BACKUP.manifest";
    let mut length_digits = 1;
    let mut maximum_length = 10;
    let rest_length = 3 + key.len() + manifest_data.len();
    while rest_length + length_digits >= maximum_length {
        length_digits += 1;
        maximum_length *= 10;
    }
    let length = rest_length + length_digits;
    let mut data = Vec::with_capacity(length);
    write!(&mut data, "{length} {key}=")?;
    data.extend_from_slice(&manifest_data);
    data.push(b'\n');
    let mut header = Header::new_ustar();
    header.set_size(data.len() as u64);
    header.set_mode(0o644);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(manifest.created_at.timestamp().try_into()?);
    header.set_entry_type(EntryType::XGlobalHeader);
    builder.append_data(&mut header, "GlobalHead.0", data.as_slice())?;
    Ok(())
}

fn append_record<W: Write>(
    builder: &mut Builder<W>,
    source: &Path,
    record: &FileRecord,
    hard_links: &mut HashMap<(u64, u64), PathBuf>,
) -> Result<()> {
    let path = if source.is_file() {
        source
    } else {
        &record.absolute
    };
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("read metadata while archiving {}", path.display()))?;
    append_xattrs(builder, &record.xattrs)?;
    let mut header = Header::new_gnu();
    header.set_metadata(&metadata);
    match record.kind {
        FileKind::Directory => {
            header.set_entry_type(EntryType::Directory);
            header.set_size(0);
            builder.append_data(&mut header, &record.relative, empty())?;
        }
        FileKind::Symlink => {
            header.set_entry_type(EntryType::Symlink);
            header.set_size(0);
            header.set_link_name(
                record
                    .link_target
                    .as_ref()
                    .context("symlink catalog entry has no target")?,
            )?;
            builder.append_data(&mut header, &record.relative, empty())?;
        }
        FileKind::File => {
            let key = (record.device, record.inode);
            if record.links > 1
                && let Some(first_path) = hard_links.get(&key)
            {
                header.set_entry_type(EntryType::Link);
                header.set_size(0);
                header.set_link_name(first_path)?;
                builder.append_data(&mut header, &record.relative, empty())?;
            } else {
                if record.links > 1 {
                    hard_links.insert(key, record.relative.clone());
                }
                let file = File::open(path)
                    .with_context(|| format!("open source file {}", path.display()))?;
                header.set_entry_type(EntryType::Regular);
                header.set_size(metadata.len());
                builder.append_data(&mut header, &record.relative, file)?;
            }
        }
    }
    Ok(())
}

fn append_xattrs<W: Write>(
    builder: &mut Builder<W>,
    attributes: &[(String, Vec<u8>)],
) -> Result<()> {
    let headers: Vec<(String, &[u8])> = attributes
        .iter()
        .map(|(name, value)| (format!("SCHILY.xattr.{name}"), value.as_slice()))
        .collect();
    builder.append_pax_extensions(headers.iter().map(|(name, value)| (name.as_str(), *value)))?;
    Ok(())
}
