use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Write, empty};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use lz4_flex::frame::FrameEncoder;
use tar::{Builder, EntryType, Header, HeaderMode};

use super::catalog::{FileKind, FileRecord};

pub fn write_compressed_tar(output: &Path, source: &Path, records: &[FileRecord]) -> Result<()> {
    let file = File::create(output).with_context(|| format!("create {}", output.display()))?;
    let encoder = FrameEncoder::new(file);
    let encoder = write_tar(encoder, source, records)?;
    encoder.finish().context("finish LZ4 frame")?.sync_all()?;
    OpenOptions::new()
        .read(true)
        .open(output)?
        .sync_all()
        .with_context(|| format!("sync {}", output.display()))?;
    Ok(())
}

fn write_tar<W: Write>(writer: W, source: &Path, records: &[FileRecord]) -> Result<W> {
    let mut builder = Builder::new(writer);
    builder.mode(HeaderMode::Complete);
    builder.follow_symlinks(false);
    let mut hard_links = HashMap::new();
    for record in records {
        append_record(&mut builder, source, record, &mut hard_links)?;
    }
    builder.finish()?;
    builder.into_inner().context("finish TAR archive")
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
